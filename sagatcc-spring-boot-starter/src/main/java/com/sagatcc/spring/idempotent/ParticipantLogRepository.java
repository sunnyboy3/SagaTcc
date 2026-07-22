package com.sagatcc.spring.idempotent;

import java.util.concurrent.Callable;

import com.sagatcc.core.message.SagaTccCommandMessage;

/** 参与方幂等处理的持久化扩展点。 */
public interface ParticipantLogRepository {

    void executeIdempotently(String localApp, SagaTccCommandMessage command,
                             Callable<Void> businessCall) throws Exception;
}
