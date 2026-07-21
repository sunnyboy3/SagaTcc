package com.sagatcc.core.api;

public interface SagaTccParticipant<T extends SagaTccRequest> {

    void sagaTry(T request);

    void sagaConfirm(T request);

    void sagaCancel(T request);
}
