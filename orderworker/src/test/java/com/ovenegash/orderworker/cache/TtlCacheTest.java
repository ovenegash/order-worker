package com.ovenegash.orderworker.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import com.ovenegash.orderworker.redis.FailedOrderStore;

class TtlCacheTest {

    static class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-18T12:00:00Z");

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @Test
    void expiresEntriesAfterTtl() {
        MutableClock clock = new MutableClock();
        TtlCache<String, String> cache = new TtlCache<>(Duration.ofSeconds(60), clock);

        cache.put("k", "v");
        assertThat(cache.get("k")).contains("v");

        clock.now = clock.now.plusSeconds(61);
        assertThat(cache.get("k")).isEmpty();
    }

    @Test
    void exponentialBackoffIsCapped() {
        Duration base = Duration.ofSeconds(10);
        Duration max = Duration.ofMinutes(1);
        assertThat(FailedOrderStore.calculateRetryDelay(1, base, max)).isEqualTo(Duration.ofSeconds(10));
        assertThat(FailedOrderStore.calculateRetryDelay(2, base, max)).isEqualTo(Duration.ofSeconds(20));
        assertThat(FailedOrderStore.calculateRetryDelay(3, base, max)).isEqualTo(Duration.ofSeconds(40));
        assertThat(FailedOrderStore.calculateRetryDelay(4, base, max)).isEqualTo(max);
    }
}
