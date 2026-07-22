package com.sagatcc.core.api;

/** 在条件不变时重试也无法成功的协议或业务失败。 */
public class SagaTccNonRetryableException extends SagaTccException {

    public SagaTccNonRetryableException(String message) {
        super(message);
    }

    public SagaTccNonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
