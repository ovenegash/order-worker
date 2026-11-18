package com.ovenegash.catalogapi.config;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import reactor.core.publisher.Mono;

@Component
public class ChaosFilter implements WebFilter {

    private final double failureRate;
    private final long delayMs;

    public ChaosFilter(@Value("${chaos.failure-rate:0}") double failureRate,
            @Value("${chaos.delay-ms:0}") long delayMs) {
        this.failureRate = failureRate;
        this.delayMs = delayMs;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        Mono<Void> delay = Mono.delay(Duration.ofMillis(delayMs)).then();

        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
            return delay.then(exchange.getResponse().setComplete());
        }
        return delay.then(chain.filter(exchange));
    }
}
