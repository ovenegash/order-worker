package com.ovenegash.orderworker.repository;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

import com.ovenegash.orderworker.domain.ProcessedOrder;

import reactor.core.publisher.Mono;

public interface ProcessedOrderRepository extends ReactiveMongoRepository<ProcessedOrder, ObjectId> {

    Mono<Boolean> existsByOrderId(String orderId);
}
