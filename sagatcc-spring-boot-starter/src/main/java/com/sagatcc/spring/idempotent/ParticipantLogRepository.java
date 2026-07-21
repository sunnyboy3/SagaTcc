package com.sagatcc.spring.idempotent;

import java.util.concurrent.Callable;

import com.sagatcc.core.message.SagaTccCommandMessage;

/** Persistence extension point for participant-side idempotency. */
public interface ParticipantLogRepository {

    void executeIdempotently(String localApp, SagaTccCommandMessage command,
                             Callable<Void> businessCall) throws Exception;
}
