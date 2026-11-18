package com.ovenegash.orderworker;

import java.time.Duration;

import com.ovenegash.orderworker.config.WorkerProperties;

public final class TestFixtures {

    private TestFixtures() {
    }

    public static WorkerProperties properties() {
        return new WorkerProperties(
                new WorkerProperties.Kafka("orders", 3),
                new WorkerProperties.Catalog("http://catalog", Duration.ofSeconds(1), Duration.ofMinutes(1)),
                new WorkerProperties.HttpRetry(3, Duration.ofMillis(1), Duration.ofMillis(5)),
                new WorkerProperties.FailedOrders(5, Duration.ofSeconds(10), Duration.ofMinutes(5), 50),
                new WorkerProperties.Lock(Duration.ofSeconds(30)));
    }
}
