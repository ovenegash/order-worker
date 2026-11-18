package com.ovenegash.orderworker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.ovenegash.orderworker.TestFixtures;
import com.ovenegash.orderworker.domain.ProcessedOrder;
import com.ovenegash.orderworker.exception.CatalogUnavailableException;
import com.ovenegash.orderworker.exception.OrderRejectedException;
import com.ovenegash.orderworker.redis.DistributedLockService;
import com.ovenegash.orderworker.redis.FailedOrderStore;
import com.ovenegash.orderworker.repository.ProcessedOrderRepository;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

class OrderProcessorTest {

    private static final String PAYLOAD =
            "{\"orderId\":\"order-123\",\"customerId\":\"customer-456\",\"products\":[\"product-789\"]}";

    private DistributedLockService lock;
    private ProcessedOrderRepository repository;
    private OrderEnrichmentService enrichment;
    private FailedOrderStore failedOrders;
    private OrderProcessor processor;

    @BeforeEach
    void setUp() {
        lock = mock(DistributedLockService.class);
        repository = mock(ProcessedOrderRepository.class);
        enrichment = mock(OrderEnrichmentService.class);
        failedOrders = mock(FailedOrderStore.class);

        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        processor = new OrderProcessor(JsonMapper.builder().build(), validator, lock, repository,
                enrichment, failedOrders, TestFixtures.properties());

        when(lock.tryAcquire(eq("order-123"), anyString(), any())).thenReturn(Mono.just(true));
        when(lock.release(eq("order-123"), anyString())).thenReturn(Mono.just(true));
        when(repository.existsByOrderId("order-123")).thenReturn(Mono.just(false));
        when(failedOrders.clear(anyString())).thenReturn(Mono.empty());
    }

    private static ProcessedOrder order() {
        return new ProcessedOrder(null, "order-123", "customer-456", List.of(), null);
    }

    @Test
    void processesAndSavesOrderAndReleasesLock() {
        when(enrichment.enrich(any())).thenReturn(Mono.just(order()));
        when(repository.save(any(ProcessedOrder.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.PROCESSED)
                .verifyComplete();

        verify(repository).save(any(ProcessedOrder.class));
        verify(failedOrders).clear("order-123");
        verify(lock).release(eq("order-123"), anyString());
    }

    @Test
    void skipsWhenAnotherInstanceHoldsTheLock() {
        when(lock.tryAcquire(eq("order-123"), anyString(), any())).thenReturn(Mono.just(false));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.ALREADY_IN_PROGRESS)
                .verifyComplete();

        verify(enrichment, never()).enrich(any());
        verify(lock, never()).release(anyString(), anyString());
    }

    @Test
    void doesNotSaveDuplicates() {
        when(repository.existsByOrderId("order-123")).thenReturn(Mono.just(true));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.DUPLICATE)
                .verifyComplete();

        verify(repository, never()).save(any(ProcessedOrder.class));
    }

    @Test
    void businessErrorsAreRejectedWithoutRetry() {
        when(enrichment.enrich(any())).thenReturn(Mono.error(new OrderRejectedException("Cliente inactivo")));
        when(failedOrders.registerRejection(eq("order-123"), eq(PAYLOAD), anyString()))
                .thenReturn(Mono.just(FailedOrderStore.FailureOutcome.DEAD_LETTER));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.REJECTED)
                .verifyComplete();

        verify(failedOrders, never()).registerTransientFailure(anyString(), anyString(), anyString());
        verify(lock).release(eq("order-123"), anyString());
    }

    @Test
    void transientErrorsAreStoredForRetry() {
        when(enrichment.enrich(any())).thenReturn(Mono.error(
                new CatalogUnavailableException("catalog down", new RuntimeException())));
        when(failedOrders.registerTransientFailure(eq("order-123"), eq(PAYLOAD), anyString()))
                .thenReturn(Mono.just(FailedOrderStore.FailureOutcome.RETRY_SCHEDULED));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.RETRY_SCHEDULED)
                .verifyComplete();
    }

    @Test
    void exhaustedRetriesGoToDeadLetter() {
        when(enrichment.enrich(any())).thenReturn(Mono.error(
                new CatalogUnavailableException("catalog down", new RuntimeException())));
        when(failedOrders.registerTransientFailure(eq("order-123"), eq(PAYLOAD), anyString()))
                .thenReturn(Mono.just(FailedOrderStore.FailureOutcome.DEAD_LETTER));

        StepVerifier.create(processor.process(PAYLOAD))
                .expectNext(ProcessingResult.DEAD_LETTER)
                .verifyComplete();
    }

    @Test
    void rejectsMalformedJsonAndInvalidMessages() {
        when(failedOrders.registerRejection(anyString(), anyString(), anyString()))
                .thenReturn(Mono.just(FailedOrderStore.FailureOutcome.DEAD_LETTER));

        StepVerifier.create(processor.process("{not json"))
                .expectNext(ProcessingResult.REJECTED)
                .verifyComplete();
        StepVerifier.create(processor.process("{\"orderId\":\"o-9\",\"customerId\":\"c\",\"products\":[]}"))
                .expectNext(ProcessingResult.REJECTED)
                .verifyComplete();

        verify(lock, never()).tryAcquire(anyString(), anyString(), any());
    }
}
