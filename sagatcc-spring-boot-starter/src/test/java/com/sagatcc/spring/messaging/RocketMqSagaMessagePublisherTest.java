package com.sagatcc.spring.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.spring.config.SagaTccProperties;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.springframework.messaging.Message;

class RocketMqSagaMessagePublisherTest {

    @Test
    void sendOkIsTheOnlyStatusAcceptedAndMessageKeyIsPreserved() {
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        SendResult result = mock(SendResult.class);
        when(result.getSendStatus()).thenReturn(SendStatus.SEND_OK);
        when(template.syncSend(eq("command-topic:COMMAND"), any(Message.class), eq(3000L)))
                .thenReturn(result);
        RocketMqSagaMessagePublisher publisher = publisher(template, new SagaTccProperties());

        publisher.publishRaw("command-topic", "COMMAND", "student-42", "{\"ok\":true}");

        ArgumentCaptor<Message> message = ArgumentCaptor.forClass(Message.class);
        verify(template).syncSend(eq("command-topic:COMMAND"), message.capture(), eq(3000L));
        assertThat(message.getValue().getHeaders().get("KEYS")).isEqualTo("student-42");
    }

    @ParameterizedTest
    @EnumSource(value = SendStatus.class, names = {
            "FLUSH_DISK_TIMEOUT", "FLUSH_SLAVE_TIMEOUT", "SLAVE_NOT_AVAILABLE"
    })
    void nonDurableBrokerStatusKeepsTheOutboxRetryable(SendStatus status) {
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        SendResult result = mock(SendResult.class);
        when(result.getSendStatus()).thenReturn(status);
        when(template.syncSend(any(String.class), any(Message.class), eq(3000L))).thenReturn(result);

        assertThatThrownBy(() -> publisher(template, new SagaTccProperties())
                .publishRaw("command-topic", "COMMAND", "key", "{}"))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining(status.name());
    }

    @Test
    void nullBrokerResultIsNotTreatedAsSuccess() {
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        when(template.syncSend(any(String.class), any(Message.class), eq(3000L))).thenReturn(null);

        assertThatThrownBy(() -> publisher(template, new SagaTccProperties())
                .publishRaw("command-topic", "COMMAND", "key", "{}"))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining("status=null");
    }

    @Test
    void derivedPerApplicationTopicIsValidatedAtTheActualTargetBoundary() {
        SagaTccProperties properties = new SagaTccProperties();
        properties.getRocketmq().setPerApplicationTopic(true);
        properties.getRocketmq().setCommandTopicPrefix(repeat('p', 120));
        RocketMqSagaMessagePublisher publisher = publisher(mock(RocketMQTemplate.class), properties);

        assertThatThrownBy(() -> publisher.commandTopic("student-service"))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining("at most 127");
    }

    @Test
    void payloadIsMeasuredInUtf8BytesBeforeCallingRocketMq() {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setMaxMessageBytes(5);
        RocketMQTemplate template = mock(RocketMQTemplate.class);
        RocketMqSagaMessagePublisher publisher = publisher(template, properties);

        assertThatThrownBy(() -> publisher.publishRaw("command-topic", "COMMAND", "key", "学生"))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining("max-message-bytes");
    }

    private RocketMqSagaMessagePublisher publisher(RocketMQTemplate template, SagaTccProperties properties) {
        return new RocketMqSagaMessagePublisher(template, properties);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
