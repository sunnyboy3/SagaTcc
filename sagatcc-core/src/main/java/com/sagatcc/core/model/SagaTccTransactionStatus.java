package com.sagatcc.core.model;

public enum SagaTccTransactionStatus {
    NEW,
    TRYING,
    COMMITTING,
    CANCELLING,
    COMMITTED,
    CANCELLED,
    FAILED
}
