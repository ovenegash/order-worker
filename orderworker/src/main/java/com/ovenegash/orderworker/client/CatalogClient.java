package com.ovenegash.orderworker.client;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.ovenegash.orderworker.cache.TtlCache;
import com.ovenegash.orderworker.config.WorkerProperties;
import com.ovenegash.orderworker.exception.CatalogUnavailableException;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Slf4j
@Component
public class CatalogClient {

    private final WebClient webClient;
    private final WorkerProperties properties;
    private final TtlCache<String, ProductDto> productCache;
    private final TtlCache<String, CustomerDto> customerCache;

    public CatalogClient(WebClient catalogWebClient, WorkerProperties properties, Clock clock) {
        this.webClient = catalogWebClient;
        this.properties = properties;
        this.productCache = new TtlCache<>(properties.catalog().cacheTtl(), clock);
        this.customerCache = new TtlCache<>(properties.catalog().cacheTtl(), clock);
    }

    public Mono<CustomerDto> getCustomer(String customerId) {
        var cachedCustomer = customerCache.get(customerId);
        if (cachedCustomer.isPresent()) {
            return Mono.just(cachedCustomer.get());
        }
        Mono<CustomerDto> request = webClient.get()
                .uri("/customers/{id}", customerId)
                .retrieve()
                .bodyToMono(CustomerDto.class)
                .onErrorResume(WebClientResponseException.NotFound.class, notFound -> Mono.empty());
        return applyRetryPolicy(request, "GET /customers/" + customerId)
                .doOnNext(customer -> customerCache.put(customerId, customer));
    }

    public Mono<Map<String, ProductDto>> getProducts(Collection<String> productIds) {
        Map<String, ProductDto> productsById = new HashMap<>();
        List<String> uncachedProductIds = new ArrayList<>();
        for (String productId : productIds) {
            productCache.get(productId).ifPresentOrElse(
                    product -> productsById.put(productId, product),
                    () -> uncachedProductIds.add(productId));
        }
        if (uncachedProductIds.isEmpty()) {
            return Mono.just(productsById);
        }
        Mono<List<ProductDto>> request = webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/products")
                        .queryParam("ids", String.join(",", uncachedProductIds))
                        .build())
                .retrieve()
                .bodyToFlux(ProductDto.class)
                .collectList();
        return applyRetryPolicy(request, "GET /products?ids=" + uncachedProductIds)
                .map(fetchedProducts -> {
                    fetchedProducts.forEach(product -> {
                        productCache.put(product.productId(), product);
                        productsById.put(product.productId(), product);
                    });
                    return productsById;
                });
    }

    private <T> Mono<T> applyRetryPolicy(Mono<T> request, String operation) {
        var retryConfig = properties.httpRetry();
        return request
                .timeout(properties.catalog().timeout())
                .retryWhen(Retry.backoff(retryConfig.maxAttempts(), retryConfig.minBackoff())
                        .maxBackoff(retryConfig.maxBackoff())
                        .jitter(0.5)
                        .filter(CatalogClient::isRetryableError)
                        .doBeforeRetry(retrySignal -> log.warn("Reintento {} de {} por: {}",
                                retrySignal.totalRetries() + 1, operation, retrySignal.failure().toString()))
                        .onRetryExhaustedThrow((retrySpec, retrySignal) -> new CatalogUnavailableException(
                                operation + " falló tras " + retrySignal.totalRetries() + " reintentos",
                                retrySignal.failure())));
    }

    static boolean isRetryableError(Throwable error) {
        if (error instanceof WebClientResponseException responseError) {
            return responseError.getStatusCode().is5xxServerError()
                    || responseError.getStatusCode().value() == 429;
        }
        return error instanceof WebClientRequestException || error instanceof TimeoutException;
    }
}
