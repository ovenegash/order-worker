package com.ovenegash.orderworker.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.ovenegash.orderworker.TestFixtures;
import com.ovenegash.orderworker.exception.CatalogUnavailableException;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class CatalogClientTest {

    private static final String CUSTOMER_JSON =
            "{\"customerId\":\"customer-456\",\"name\":\"Juan\",\"email\":\"j@mail.com\",\"active\":true}";

    private final AtomicInteger requestCount = new AtomicInteger();

    private CatalogClient clientRespondingWith(ExchangeFunction exchange) {
        WebClient webClient = WebClient.builder().baseUrl("http://catalog").exchangeFunction(exchange).build();
        return new CatalogClient(webClient, TestFixtures.properties(), Clock.systemUTC());
    }

    private static Mono<ClientResponse> json(HttpStatus status, String body) {
        return Mono.just(ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build());
    }

    @Test
    void retriesTransientErrorsAndThenSucceeds() {
        CatalogClient client = clientRespondingWith(request -> requestCount.incrementAndGet() < 3
                ? json(HttpStatus.SERVICE_UNAVAILABLE, "")
                : json(HttpStatus.OK, CUSTOMER_JSON));

        StepVerifier.create(client.getCustomer("customer-456"))
                .assertNext(customer -> assertThat(customer.active()).isTrue())
                .verifyComplete();
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void failsWithCatalogUnavailableAfterExhaustingRetries() {
        CatalogClient client = clientRespondingWith(request -> {
            requestCount.incrementAndGet();
            return json(HttpStatus.SERVICE_UNAVAILABLE, "");
        });

        StepVerifier.create(client.getCustomer("customer-456"))
                .expectError(CatalogUnavailableException.class)
                .verify();
        assertThat(requestCount).hasValue(4); // 1 intento + 3 reintentos
    }

    @Test
    void notFoundIsEmptyAndNotRetried() {
        CatalogClient client = clientRespondingWith(request -> {
            requestCount.incrementAndGet();
            return json(HttpStatus.NOT_FOUND, "");
        });

        StepVerifier.create(client.getCustomer("nope")).verifyComplete();
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void cachesProductsBetweenCalls() {
        CatalogClient client = clientRespondingWith(request -> {
            requestCount.incrementAndGet();
            return json(HttpStatus.OK,
                    "[{\"productId\":\"product-789\",\"name\":\"Laptop\",\"description\":\"x\",\"price\":999}]");
        });

        StepVerifier.create(client.getProducts(List.of("product-789")))
                .assertNext(productsById -> assertThat(productsById).containsKey("product-789"))
                .verifyComplete();
        StepVerifier.create(client.getProducts(List.of("product-789")))
                .assertNext(productsById -> assertThat(productsById.get("product-789").name()).isEqualTo("Laptop"))
                .verifyComplete();
        assertThat(requestCount).hasValue(1);
    }
}
