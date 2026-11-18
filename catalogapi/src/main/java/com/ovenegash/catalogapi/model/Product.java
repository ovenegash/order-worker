package com.ovenegash.catalogapi.model;

import java.math.BigDecimal;

public record Product(String productId, String name, String description, BigDecimal price) {
}
