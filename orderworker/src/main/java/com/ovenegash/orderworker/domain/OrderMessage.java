package com.ovenegash.orderworker.domain;

import java.util.List;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

public record OrderMessage(
        @NotBlank String orderId,
        @NotBlank String customerId,
        @NotEmpty List<@NotBlank String> products) {
}
