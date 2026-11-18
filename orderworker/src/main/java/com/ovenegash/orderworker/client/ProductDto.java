package com.ovenegash.orderworker.client;

import java.math.BigDecimal;

public record ProductDto(String productId, String name, String description, BigDecimal price) {
}
