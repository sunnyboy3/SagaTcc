package com.sagatcc.core.api;

public class SagaTccException extends RuntimeException {

    public SagaTccException(String message) {
        super(message);
    }

    public SagaTccException(String message, Throwable cause) {
        super(message, cause);
    }
}
