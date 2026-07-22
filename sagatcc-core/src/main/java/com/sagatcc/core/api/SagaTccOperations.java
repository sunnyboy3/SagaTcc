package com.sagatcc.core.api;

public interface SagaTccOperations {

    String begin(String businessCode, String businessId);

    String begin(String businessCode, String businessId, String sagaId);

    void enlist(SagaTccRequest request);

    /**
     * 登记一个分支，无需请求 DTO 携带 {@link SagaTccBusiness}。
     * 适用于业务 DTO 需要与 SagaTcc API 保持解耦的场景。
     */
    default void enlist(String targetApplication, String businessCode, SagaTccRequest request) {
        throw new UnsupportedOperationException("explicit SagaTcc branch routing is not supported by this implementation");
    }

    String currentSagaId();
}
