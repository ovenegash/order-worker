package com.ovenegash.orderworker.client;

public record CustomerDto(String customerId, String name, String email, boolean active) {
}
