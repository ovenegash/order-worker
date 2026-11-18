package com.ovenegash.orderworker.domain;

import java.math.BigDecimal;

import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

public record OrderProduct(
        String productId,
        String name,
        @Field(targetType = FieldType.DECIMAL128) BigDecimal price) {
}
