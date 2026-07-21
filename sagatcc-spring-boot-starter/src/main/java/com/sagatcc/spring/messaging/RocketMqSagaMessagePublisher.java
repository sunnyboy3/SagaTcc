package com.sagatcc.spring.messaging;

import java.nio.charset.StandardCharsets;

import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.config.SagaTccNameResolver;
import com.sagatcc.core.api.SagaTccException;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;

public class RocketMqSagaMessagePublisher implements SagaMessagePublisher {

    public static final String COMMAND_TAG = SagaMessagePublisher.COMMAND_TAG;
    public static final String RESULT_TAG = SagaMessagePublisher.RESULT_TAG;

    private final RocketMQTemplate rocketMQTemplate;
    private final SagaTccProperties properties;

    public RocketMqSagaMessagePublisher(RocketMQTemplate rocketMQTemplate, SagaTccProperties properties) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.properties = properties;
    }

    @Override
    public String commandTopic(String targetApp) {
        if (properties.getRocketmq().isPerApplicationTopic()) {
            return requireValidTopic(properties.getRocketmq().getCommandTopicPrefix() + targetApp);
        }
        return requireValidTopic(properties.getRocketmq().getCommandTopic());
    }

    @Override
    public String resultTopic(String coordinatorApp) {
        if (properties.getRocketmq().isPerApplicationTopic()) {
            return requireValidTopic(properties.getRocketmq().getResultTopicPrefix() + coordinatorApp);
        }
        return requireValidTopic(properties.getRocketmq().getResultTopic());
    }

    @Override
    public void publishRaw(String topic, String tag, String messageKey, String payload) {
        requireValidTopic(topic);
        if (payload == null || payload.getBytes(StandardCharsets.UTF_8).length > properties.getMaxMessageBytes()) {
            throw new SagaTccException("SagaTcc message exceeds sagatcc.max-message-bytes");
        }
        String destination = topic + ":" + tag;
        SendResult result = rocketMQTemplate.syncSend(destination,
                MessageBuilder.withPayload(payload)
                        .setHeader("KEYS", messageKey)
                        .build(),
                properties.getRocketmq().getSendTimeoutMillis());
        if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
            throw new SagaTccException("RocketMQ did not durably accept SagaTcc message, status="
                    + (result == null ? "null" : result.getSendStatus()));
        }
    }

    private String requireValidTopic(String topic) {
        if (topic == null || topic.length() == 0 || topic.length() > 127
                || !SagaTccNameResolver.isRocketMqName(topic)) {
            throw new SagaTccException("invalid RocketMQ topic; derived topic must use [%|a-zA-Z0-9_-] "
                    + "and contain at most 127 characters");
        }
        return topic;
    }
}
