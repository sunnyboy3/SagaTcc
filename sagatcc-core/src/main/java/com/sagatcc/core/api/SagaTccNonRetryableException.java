package com.sagatcc.core.api;

/** A protocol or business failure that cannot succeed when retried unchanged. */
public class SagaTccNonRetryableException extends SagaTccException {

    public SagaTccNonRetryableException(String message) {
        super(message);
    }

    public SagaTccNonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
