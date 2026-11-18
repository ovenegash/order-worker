package com.ovenegash.orderworker.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ovenegash.orderworker.service.OrderProcessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderListener {

    private final OrderProcessor processor;

    @KafkaListener(topics = "${worker.kafka.topic}")
    public Mono<Void> onMessage(ConsumerRecord<String, String> record) {
        log.debug("Mensaje recibido partition={} offset={} key={}",
                record.partition(), record.offset(), record.key());
        return processor.process(record.value()).then();
    }
}
