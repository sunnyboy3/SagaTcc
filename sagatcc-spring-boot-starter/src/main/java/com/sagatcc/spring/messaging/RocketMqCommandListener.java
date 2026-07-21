package com.sagatcc.spring.messaging;

import com.sagatcc.spring.participant.SagaTccParticipantDispatcher;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;

@RocketMQMessageListener(
        topic = "${sagatcc.rocketmq.command-topic:sagatcc-command}",
        consumerGroup = "${sagatcc.rocketmq.command-consumer-group:${spring.application.name}-sagatcc-command}")
public class RocketMqCommandListener implements RocketMQListener<String> {

    private final SagaTccParticipantDispatcher dispatcher;

    public RocketMqCommandListener(SagaTccParticipantDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onMessage(String message) {
        dispatcher.dispatch(message);
    }
}
