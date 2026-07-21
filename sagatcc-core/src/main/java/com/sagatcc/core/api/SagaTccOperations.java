package com.sagatcc.core.api;

public interface SagaTccOperations {

    String begin(String businessCode, String businessId);

    String begin(String businessCode, String businessId, String sagaId);

    void enlist(SagaTccRequest request);

    /**
     * Enlists a branch without requiring the request DTO to carry
     * {@link SagaTccBusiness}. This is useful when business DTOs must remain
     * independent from the SagaTcc API.
     */
    default void enlist(String targetApplication, String businessCode, SagaTccRequest request) {
        throw new UnsupportedOperationException("explicit SagaTcc branch routing is not supported by this implementation");
    }

    String currentSagaId();
}
