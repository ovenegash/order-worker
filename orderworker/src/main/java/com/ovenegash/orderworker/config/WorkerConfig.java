package com.ovenegash.orderworker.config;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerConfig {

    @Bean
    WebClient catalogWebClient(WorkerProperties properties) {
        return WebClient.builder()
                .baseUrl(properties.catalog().baseUrl())
                .build();
    }

    @Bean
    NewTopic ordersTopic(WorkerProperties properties) {
        return TopicBuilder.name(properties.kafka().topic())
                .partitions(properties.kafka().partitions())
                .replicas(1)
                .build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
