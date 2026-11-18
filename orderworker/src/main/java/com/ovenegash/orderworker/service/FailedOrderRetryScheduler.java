package com.ovenegash.orderworker.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ovenegash.orderworker.redis.FailedOrderStore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class FailedOrderRetryScheduler {

    private final FailedOrderStore failedOrderStore;
    private final OrderProcessor orderProcessor;

    // Spring se re-suscribe al mismo Mono en cada ejecución, por eso el defer
    @Scheduled(fixedDelayString = "${worker.failed-orders.poll-interval-ms:5000}")
    public Mono<Void> retryDueOrders() {
        return Mono.defer(this::retryPendingOrders);
    }

    private Mono<Void> retryPendingOrders() {
        return failedOrderStore.findOrdersReadyToRetry()
                .concatMap(orderId -> failedOrderStore.getPayload(orderId)
                        .doOnNext(payload -> log.info("Reintentando pedido {}", orderId))
                        .flatMap(orderProcessor::process)
                        .onErrorResume(error -> {
                            log.error("Error reintentando pedido {}", orderId, error);
                            return Mono.empty();
                        }))
                .then()
                .onErrorResume(error -> {
                    log.error("No se pudo leer la cola de reintentos", error);
                    return Mono.empty();
                });
    }
}
