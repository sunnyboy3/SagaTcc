package com.sagatcc.core.api;

/**
 * 在结果返回协调器之前对参与方失败进行分类。
 * 应用可以提供自定义实现，避免永久性业务失败被重复重试。
 */
public interface SagaTccFailureClassifier {

    boolean isRetryable(Throwable failure);
}
