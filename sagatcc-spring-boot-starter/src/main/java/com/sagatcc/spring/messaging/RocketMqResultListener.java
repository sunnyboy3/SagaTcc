package com.sagatcc.spring.messaging;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;

@RocketMQMessageListener(
        topic = "${sagatcc.rocketmq.result-topic:sagatcc-result}",
        consumerGroup = "${sagatcc.rocketmq.result-consumer-group:${spring.application.name}-sagatcc-result}")
public class RocketMqResultListener implements RocketMQListener<String> {

    private final ObjectMapper objectMapper;
    private final SagaTccCoordinator coordinator;
    private final String localApplication;
    private final int maxMessageBytes;

    public RocketMqResultListener(ObjectMapper objectMapper, SagaTccCoordinator coordinator) {
        this(objectMapper, coordinator, null, Integer.MAX_VALUE);
    }

    public RocketMqResultListener(ObjectMapper objectMapper, SagaTccCoordinator coordinator,
                                  String localApplication) {
        this(objectMapper, coordinator, localApplication, Integer.MAX_VALUE);
    }

    public RocketMqResultListener(ObjectMapper objectMapper, SagaTccCoordinator coordinator,
                                  String localApplication, int maxMessageBytes) {
        this.objectMapper = objectMapper;
        this.coordinator = coordinator;
        this.localApplication = localApplication;
        this.maxMessageBytes = maxMessageBytes;
    }

    @Override
    public void onMessage(String message) {
        try {
            if (message == null || message.getBytes(StandardCharsets.UTF_8).length > maxMessageBytes) {
                throw new SagaTccException("SagaTcc result exceeds max-message-bytes");
            }
            SagaTccResultMessage result = objectMapper.readValue(message, SagaTccResultMessage.class);
            if (result == null || !validText(result.getSagaId(), 64) || result.getBranchId() == null
                    || result.getBranchId() <= 0 || result.getAction() == null || result.getAttempt() <= 0
                    || !result.successSpecified() || !result.retryableSpecified()
                    || !validText(result.getCoordinatorApp(), 128)
                    || !validText(result.getTargetApp(), 128) || !validText(result.getBusCode(), 128)
                    || (localApplication != null
                    && !localApplication.equals(result.getCoordinatorApp()))) {
                return;
            }
            coordinator.handleResult(result);
        } catch (SagaTccException e) {
            throw e;
        } catch (Exception e) {
            throw new SagaTccException("handle SagaTcc result failed", e);
        }
    }

    private boolean validText(String value, int maxLength) {
        return value != null && value.trim().length() > 0 && value.length() <= maxLength;
    }
}
