package com.ovenegash.orderworker.redis;

import java.time.Duration;
import java.util.List;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class DistributedLockService {

    private static final String PREFIX = "lock:order:";

    // solo borra el lock si el token es el de quien lo tomó
    private static final RedisScript<Long> RELEASE_SCRIPT = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;

    public Mono<Boolean> tryAcquire(String orderId, String token, Duration ttl) {
        return redis.opsForValue()
                .setIfAbsent(PREFIX + orderId, token, ttl)
                .defaultIfEmpty(false);
    }

    public Mono<Boolean> release(String orderId, String token) {
        return redis.execute(RELEASE_SCRIPT, List.of(PREFIX + orderId), List.of(token))
                .next()
                .map(deleted -> deleted == 1L)
                .defaultIfEmpty(false);
    }
}
