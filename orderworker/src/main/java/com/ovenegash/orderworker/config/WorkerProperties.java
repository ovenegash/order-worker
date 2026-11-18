package com.ovenegash.orderworker.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "worker")
public record WorkerProperties(
        Kafka kafka,
        Catalog catalog,
        HttpRetry httpRetry,
        FailedOrders failedOrders,
        Lock lock) {

    public record Kafka(String topic, int partitions) {
    }

    public record Catalog(String baseUrl, Duration timeout, Duration cacheTtl) {
    }

    public record HttpRetry(int maxAttempts, Duration minBackoff, Duration maxBackoff) {
    }

    public record FailedOrders(int maxAttempts, Duration baseDelay, Duration maxDelay, long batchSize) {
    }

    public record Lock(Duration ttl) {
    }
}
