package com.ovenegash.orderworker.redis;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;

import com.ovenegash.orderworker.config.WorkerProperties;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class FailedOrderStore {

    public static final String FAILED_PREFIX = "orders:failed:";
    public static final String RETRY_QUEUE = "orders:retry-queue";
    public static final String DEAD_LETTER = "orders:dead-letter";

    public enum FailureOutcome {
        RETRY_SCHEDULED, DEAD_LETTER
    }

    private final ReactiveStringRedisTemplate redis;
    private final ReactiveHashOperations<String, String, String> hashOps;
    private final WorkerProperties.FailedOrders failedOrdersConfig;
    private final Clock clock;

    public FailedOrderStore(ReactiveStringRedisTemplate redis, WorkerProperties properties, Clock clock) {
        this.redis = redis;
        this.hashOps = redis.opsForHash();
        this.failedOrdersConfig = properties.failedOrders();
        this.clock = clock;
    }

    public Mono<FailureOutcome> registerTransientFailure(String orderId, String payload, String errorMessage) {
        String key = FAILED_PREFIX + orderId;
        Instant now = clock.instant();
        return hashOps.increment(key, "attempts", 1)
                .flatMap(attempts -> {
                    boolean maxAttemptsReached = attempts >= failedOrdersConfig.maxAttempts();
                    Map<String, String> fields = Map.of(
                            "payload", payload,
                            "lastError", String.valueOf(errorMessage),
                            "status", maxAttemptsReached ? "DEAD" : "RETRYING",
                            "updatedAt", now.toString());
                    Mono<Boolean> saveFields = hashOps.putAll(key, fields);
                    if (maxAttemptsReached) {
                        return saveFields.then(moveToDeadLetter(orderId)).thenReturn(FailureOutcome.DEAD_LETTER);
                    }
                    Duration retryDelay = calculateRetryDelay(attempts,
                            failedOrdersConfig.baseDelay(), failedOrdersConfig.maxDelay());
                    long nextRetryAt = now.plus(retryDelay).toEpochMilli();
                    return saveFields.then(redis.opsForZSet().add(RETRY_QUEUE, orderId, nextRetryAt))
                            .thenReturn(FailureOutcome.RETRY_SCHEDULED);
                });
    }

    public Mono<FailureOutcome> registerRejection(String orderId, String payload, String reason) {
        Map<String, String> fields = Map.of(
                "payload", payload,
                "lastError", reason,
                "status", "REJECTED",
                "updatedAt", clock.instant().toString());
        return hashOps.putAll(FAILED_PREFIX + orderId, fields)
                .then(moveToDeadLetter(orderId))
                .thenReturn(FailureOutcome.DEAD_LETTER);
    }

    public Mono<Void> clear(String orderId) {
        return redis.delete(FAILED_PREFIX + orderId)
                .then(redis.opsForZSet().remove(RETRY_QUEUE, orderId))
                .then(redis.opsForSet().remove(DEAD_LETTER, orderId))
                .then();
    }

    public Flux<String> findOrdersReadyToRetry() {
        // defer para que la hora se calcule en cada ejecución del scheduler
        return Flux.defer(() -> redis.opsForZSet().rangeByScore(RETRY_QUEUE,
                Range.closed(0.0, (double) clock.millis()),
                Limit.limit().count((int) failedOrdersConfig.batchSize())));
    }

    public Mono<String> getPayload(String orderId) {
        return hashOps.get(FAILED_PREFIX + orderId, "payload");
    }

    public static Duration calculateRetryDelay(long attempt, Duration baseDelay, Duration maxDelay) {
        long exponent = Math.min(Math.max(attempt - 1, 0), 30);
        Duration delay = baseDelay.multipliedBy(1L << exponent);
        return delay.compareTo(maxDelay) > 0 ? maxDelay : delay;
    }

    private Mono<Void> moveToDeadLetter(String orderId) {
        return redis.opsForZSet().remove(RETRY_QUEUE, orderId)
                .then(redis.opsForSet().add(DEAD_LETTER, orderId))
                .then();
    }
}
