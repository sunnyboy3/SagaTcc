package com.sagatcc.core.api;

/**
 * Classifies participant failures before a result is returned to the
 * coordinator. Applications may provide their own implementation to stop
 * retrying permanent business failures.
 */
public interface SagaTccFailureClassifier {

    boolean isRetryable(Throwable failure);
}
