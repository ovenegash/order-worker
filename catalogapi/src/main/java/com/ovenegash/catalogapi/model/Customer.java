package com.ovenegash.catalogapi.model;

public record Customer(String customerId, String name, String email, boolean active) {
}
