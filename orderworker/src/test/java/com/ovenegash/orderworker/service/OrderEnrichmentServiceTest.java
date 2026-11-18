package com.ovenegash.orderworker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ovenegash.orderworker.client.CatalogClient;
import com.ovenegash.orderworker.client.CustomerDto;
import com.ovenegash.orderworker.client.ProductDto;
import com.ovenegash.orderworker.domain.OrderMessage;
import com.ovenegash.orderworker.domain.OrderProduct;
import com.ovenegash.orderworker.exception.OrderRejectedException;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class OrderEnrichmentServiceTest {

    private static final ProductDto LAPTOP =
            new ProductDto("product-789", "Laptop", "Laptop 15", new BigDecimal("999"));

    private CatalogClient catalogClient;
    private OrderEnrichmentService service;

    @BeforeEach
    void setUp() {
        catalogClient = mock(CatalogClient.class);
        service = new OrderEnrichmentService(catalogClient,
                Clock.fixed(Instant.parse("2026-11-18T12:00:00Z"), ZoneOffset.UTC));
    }

    @Test
    void enrichesValidOrder() {
        when(catalogClient.getCustomer("customer-456"))
                .thenReturn(Mono.just(new CustomerDto("customer-456", "Juan", "j@mail.com", true)));
        when(catalogClient.getProducts(anyCollection())).thenReturn(Mono.just(Map.of("product-789", LAPTOP)));

        StepVerifier.create(service.enrich(new OrderMessage("order-123", "customer-456", List.of("product-789"))))
                .assertNext(processedOrder -> {
                    assertThat(processedOrder.orderId()).isEqualTo("order-123");
                    assertThat(processedOrder.customerId()).isEqualTo("customer-456");
                    assertThat(processedOrder.products()).containsExactly(
                            new OrderProduct("product-789", "Laptop", new BigDecimal("999")));
                })
                .verifyComplete();
    }

    @Test
    void rejectsInactiveCustomer() {
        when(catalogClient.getCustomer("customer-999"))
                .thenReturn(Mono.just(new CustomerDto("customer-999", "Ana", "a@mail.com", false)));
        when(catalogClient.getProducts(anyCollection())).thenReturn(Mono.just(Map.of("product-789", LAPTOP)));

        StepVerifier.create(service.enrich(new OrderMessage("o-1", "customer-999", List.of("product-789"))))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(OrderRejectedException.class)
                        .hasMessageContaining("inactivo"))
                .verify();
    }

    @Test
    void rejectsUnknownCustomer() {
        when(catalogClient.getCustomer("ghost")).thenReturn(Mono.empty());
        when(catalogClient.getProducts(anyCollection())).thenReturn(Mono.just(Map.of("product-789", LAPTOP)));

        StepVerifier.create(service.enrich(new OrderMessage("o-1", "ghost", List.of("product-789"))))
                .expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("no existe"))
                .verify();
    }

    @Test
    void rejectsMissingProducts() {
        when(catalogClient.getCustomer("customer-456"))
                .thenReturn(Mono.just(new CustomerDto("customer-456", "Juan", "j@mail.com", true)));
        when(catalogClient.getProducts(anyCollection())).thenReturn(Mono.just(Map.of("product-789", LAPTOP)));

        StepVerifier.create(service.enrich(
                new OrderMessage("o-1", "customer-456", List.of("product-789", "product-000"))))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(OrderRejectedException.class)
                        .hasMessageContaining("product-000"))
                .verify();
    }
}
