package com.ovenegash.orderworker.domain;

import java.time.Instant;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("orders")
public record ProcessedOrder(
        @Id ObjectId id,
        @Indexed(unique = true) String orderId,
        @Indexed String customerId,
        List<OrderProduct> products,
        Instant processedAt) {

    public ProcessedOrder withId(ObjectId newId) {
        return new ProcessedOrder(newId, orderId, customerId, products, processedAt);
    }
}
