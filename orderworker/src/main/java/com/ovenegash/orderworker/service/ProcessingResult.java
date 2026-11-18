package com.ovenegash.orderworker.service;

public enum ProcessingResult {
    PROCESSED,
    DUPLICATE,
    ALREADY_IN_PROGRESS,
    REJECTED,
    RETRY_SCHEDULED,
    DEAD_LETTER
}
