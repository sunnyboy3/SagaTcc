package com.sagatcc.spring.messaging;

public interface SagaMessagePublisher {

    String COMMAND_TAG = "COMMAND";
    String RESULT_TAG = "RESULT";

    String commandTopic(String targetApp);

    String resultTopic(String coordinatorApp);

    void publishRaw(String topic, String tag, String messageKey, String payload);
}
