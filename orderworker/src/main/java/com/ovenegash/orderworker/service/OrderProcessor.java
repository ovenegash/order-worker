package com.ovenegash.orderworker.service;

import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.ovenegash.orderworker.config.WorkerProperties;
import com.ovenegash.orderworker.domain.OrderMessage;
import com.ovenegash.orderworker.exception.OrderRejectedException;
import com.ovenegash.orderworker.redis.DistributedLockService;
import com.ovenegash.orderworker.redis.FailedOrderStore;
import com.ovenegash.orderworker.repository.ProcessedOrderRepository;

import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessor {

    private final JsonMapper jsonMapper;
    private final Validator validator;
    private final DistributedLockService lockService;
    private final ProcessedOrderRepository orderRepository;
    private final OrderEnrichmentService enrichmentService;
    private final FailedOrderStore failedOrderStore;
    private final WorkerProperties properties;

    public Mono<ProcessingResult> process(String payload) {
        OrderMessage order;
        try {
            order = jsonMapper.readValue(payload, OrderMessage.class);
        } catch (JacksonException parseError) {
            log.error("Mensaje con JSON inválido descartado: {}", parseError.getOriginalMessage());
            return failedOrderStore.registerRejection("invalid-" + UUID.randomUUID(), payload,
                    "JSON inválido: " + parseError.getOriginalMessage())
                    .thenReturn(ProcessingResult.REJECTED);
        }

        String validationErrors = validator.validate(order).stream()
                .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));
        if (!validationErrors.isEmpty()) {
            String orderId = order.orderId() == null || order.orderId().isBlank()
                    ? "invalid-" + UUID.randomUUID()
                    : order.orderId();
            log.error("Pedido {} inválido: {}", orderId, validationErrors);
            return failedOrderStore.registerRejection(orderId, payload, "Mensaje inválido: " + validationErrors)
                    .thenReturn(ProcessingResult.REJECTED);
        }
        return process(order, payload);
    }

    Mono<ProcessingResult> process(OrderMessage order, String payload) {
        String orderId = order.orderId();
        String lockToken = UUID.randomUUID().toString();

        // si Redis o Mongo fallan el error sube a Kafka y el mensaje se vuelve a entregar
        return Mono.usingWhen(
                lockService.tryAcquire(orderId, lockToken, properties.lock().ttl()),
                lockAcquired -> lockAcquired
                        ? processWithLock(order, payload)
                        : Mono.fromSupplier(() -> {
                            log.info("Pedido {} bloqueado por otra instancia, se omite", orderId);
                            return ProcessingResult.ALREADY_IN_PROGRESS;
                        }),
                lockAcquired -> lockAcquired ? lockService.release(orderId, lockToken).then() : Mono.empty());
    }

    private Mono<ProcessingResult> processWithLock(OrderMessage order, String payload) {
        String orderId = order.orderId();
        return orderRepository.existsByOrderId(orderId)
                .flatMap(alreadySaved -> alreadySaved
                        ? Mono.just(ProcessingResult.DUPLICATE)
                        : enrichAndSave(order))
                .flatMap(result -> failedOrderStore.clear(orderId).thenReturn(result))
                .doOnNext(result -> log.info("Pedido {} -> {}", orderId, result))
                .onErrorResume(OrderRejectedException.class, rejection -> {
                    log.warn("Pedido {} rechazado: {}", orderId, rejection.getMessage());
                    return failedOrderStore.registerRejection(orderId, payload, rejection.getMessage())
                            .thenReturn(ProcessingResult.REJECTED);
                })
                .onErrorResume(error -> !(error instanceof OrderRejectedException), error -> {
                    log.warn("Pedido {} falló, se reintentará: {}", orderId, error.getMessage());
                    return failedOrderStore.registerTransientFailure(orderId, payload, error.getMessage())
                            .map(outcome -> outcome == FailedOrderStore.FailureOutcome.DEAD_LETTER
                                    ? ProcessingResult.DEAD_LETTER
                                    : ProcessingResult.RETRY_SCHEDULED);
                });
    }

    private Mono<ProcessingResult> enrichAndSave(OrderMessage order) {
        return enrichmentService.enrich(order)
                .flatMap(orderRepository::save)
                .thenReturn(ProcessingResult.PROCESSED)
                .onErrorResume(DuplicateKeyException.class, duplicate -> Mono.just(ProcessingResult.DUPLICATE));
    }
}
