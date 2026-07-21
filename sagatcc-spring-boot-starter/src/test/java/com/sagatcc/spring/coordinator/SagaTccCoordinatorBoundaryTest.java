package com.sagatcc.spring.coordinator;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccEnlistment;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.store.SagaTccRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorBoundaryTest {

    private SagaTccRepository repository;
    private SagaMessagePublisher publisher;
    private ObjectMapper objectMapper;
    private SagaTccProperties properties;

    @BeforeEach
    void setUp() {
        repository = mock(SagaTccRepository.class);
        publisher = mock(SagaMessagePublisher.class);
        objectMapper = new ObjectMapper();
        properties = new SagaTccProperties();
        when(repository.insertBranch(anyString(), eq(1), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(9L);
        when(publisher.commandTopic(anyString())).thenReturn("command-topic");
    }

    @Test
    void acceptsAUtf8RequestWhoseSerializedBytesExactlyMatchTheLimit() throws Exception {
        PayloadRequest request = new PayloadRequest("student-学生");
        String expectedJson = objectMapper.writeValueAsString(request);
        properties.setMaxRequestBytes(expectedJson.getBytes(StandardCharsets.UTF_8).length);
        SagaTccCoordinator coordinator = coordinator();

        coordinator.persistAndScheduleTry(contextWith(request));

        ArgumentCaptor<String> requestJson = ArgumentCaptor.forClass(String.class);
        verify(repository).insertBranch(eq("saga"), eq(1), eq("wallet"), eq("reserve"),
                eq(PayloadRequest.class.getName()), requestJson.capture());
        assertEquals(expectedJson, requestJson.getValue());
        verify(repository).markActionDispatched(9L, SagaTccBranchStatus.TRYING, 1);
        verify(repository).updateTransactionStatus("saga", SagaTccTransactionStatus.TRYING);
    }

    @Test
    void rejectsARequestEvenOneUtf8ByteOverTheLimitBeforeCreatingABranchOrOutbox() throws Exception {
        PayloadRequest request = new PayloadRequest("学");
        int serializedBytes = objectMapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8).length;
        properties.setMaxRequestBytes(serializedBytes - 1);
        SagaTccCoordinator coordinator = coordinator();

        SagaTccException error = assertThrows(SagaTccException.class,
                () -> coordinator.persistAndScheduleTry(contextWith(request)));

        assertTrue(error.getMessage().contains("max-request-bytes"));
        verify(repository).insertTransaction(org.mockito.ArgumentMatchers.any(SagaTccContext.class), eq(1));
        verify(repository, never()).insertBranch(anyString(), eq(1), anyString(), anyString(), anyString(), anyString());
        verify(repository, never()).enqueueOutbox(anyString(), anyString(), org.mockito.ArgumentMatchers.anyLong(),
                anyString(), anyString(), org.mockito.ArgumentMatchers.any(SagaTccAction.class),
                org.mockito.ArgumentMatchers.anyInt(), anyString());
    }

    @Test
    void acceptsCombinedUtf8PayloadWhoseSerializedBytesExactlyMatchTheSagaLimit() throws Exception {
        PayloadRequest first = new PayloadRequest("学生-one");
        PayloadRequest second = new PayloadRequest("学生-two");
        int firstBytes = serializedBytes(first);
        int secondBytes = serializedBytes(second);
        properties.setMaxRequestBytes(Math.max(firstBytes, secondBytes));
        properties.setMaxSagaPayloadBytes(firstBytes + secondBytes);
        when(repository.insertBranch(eq("saga"), eq(2), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(10L);

        coordinator().persistAndScheduleTry(contextWith(first, second));

        verify(repository).insertBranch(eq("saga"), eq(1), eq("wallet"), eq("reserve"),
                eq(PayloadRequest.class.getName()), anyString());
        verify(repository).insertBranch(eq("saga"), eq(2), eq("wallet"), eq("reserve"),
                eq(PayloadRequest.class.getName()), anyString());
        verify(repository).updateTransactionStatus("saga", SagaTccTransactionStatus.TRYING);
    }

    @Test
    void rejectsCombinedPayloadOneByteOverTheSagaLimit() throws Exception {
        PayloadRequest first = new PayloadRequest("学生-one");
        PayloadRequest second = new PayloadRequest("学生-two");
        int firstBytes = serializedBytes(first);
        int secondBytes = serializedBytes(second);
        properties.setMaxRequestBytes(Math.max(firstBytes, secondBytes));
        properties.setMaxSagaPayloadBytes(firstBytes + secondBytes - 1);

        SagaTccException error = assertThrows(SagaTccException.class,
                () -> coordinator().persistAndScheduleTry(contextWith(first, second)));

        assertTrue(error.getMessage().contains("max-saga-payload-bytes"));
        verify(repository).insertBranch(eq("saga"), eq(1), eq("wallet"), eq("reserve"),
                eq(PayloadRequest.class.getName()), anyString());
        verify(repository, never()).insertBranch(eq("saga"), eq(2), anyString(), anyString(), anyString(), anyString());
        verify(repository, never()).updateTransactionStatus("saga", SagaTccTransactionStatus.TRYING);
    }

    @Test
    void finalCommandUtf8SizeAcceptsTheLimitAndRejectsOneByteLess() throws Exception {
        PayloadRequest request = new PayloadRequest("学生-command-boundary");
        int commandBytes = commandBytes(request);
        properties.setMaxMessageBytes(commandBytes);

        coordinator().persistAndScheduleTry(contextWith(request));

        verify(repository).enqueueOutbox(eq("saga-9-TRY-1"), eq("saga"), eq(9L), eq("command-topic"),
                eq(SagaMessagePublisher.COMMAND_TAG), eq(SagaTccAction.TRY), eq(1), anyString());

        repository = mock(SagaTccRepository.class);
        when(repository.insertBranch(anyString(), eq(1), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(9L);
        properties.setMaxMessageBytes(commandBytes - 1);

        SagaTccException error = assertThrows(SagaTccException.class,
                () -> coordinator().persistAndScheduleTry(contextWith(request)));
        assertTrue(error.getMessage().contains("max-message-bytes"));
        verify(repository, never()).enqueueOutbox(anyString(), anyString(),
                org.mockito.ArgumentMatchers.anyLong(), anyString(), anyString(),
                org.mockito.ArgumentMatchers.any(SagaTccAction.class),
                org.mockito.ArgumentMatchers.anyInt(), anyString());
    }

    @Test
    void aSagaWithNoBranchesCommitsWithoutCreatingOutboxWork() {
        SagaTccContext context = new SagaTccContext("empty-saga", "order", "create-order", "student-1");

        coordinator().persistAndScheduleTry(context);

        verify(repository).insertTransaction(context, 0);
        verify(repository).updateTransactionStatus("empty-saga", SagaTccTransactionStatus.COMMITTED);
        verify(repository, never()).insertBranch(anyString(), eq(1), anyString(), anyString(), anyString(), anyString());
        verify(repository, never()).enqueueOutbox(anyString(), anyString(), org.mockito.ArgumentMatchers.anyLong(),
                anyString(), anyString(), org.mockito.ArgumentMatchers.any(SagaTccAction.class),
                org.mockito.ArgumentMatchers.anyInt(), anyString());
    }

    @Test
    void anAlreadyPersistedContextIsIdempotentlyIgnored() {
        SagaTccContext context = contextWith(new PayloadRequest("value"));
        context.setPersisted(true);

        coordinator().persistAndScheduleTry(context);

        verifyNoInteractions(repository, publisher);
    }

    private SagaTccCoordinator coordinator() {
        return new SagaTccCoordinator(repository, publisher, objectMapper, properties, "order");
    }

    private int serializedBytes(SagaTccRequest request) throws Exception {
        return objectMapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8).length;
    }

    private int commandBytes(SagaTccRequest request) throws Exception {
        String requestJson = objectMapper.writeValueAsString(request);
        SagaTccCommandMessage message = new SagaTccCommandMessage();
        message.setSagaId("saga");
        message.setBranchId(9L);
        message.setCoordinatorApp("order");
        message.setTargetApp("wallet");
        message.setBusCode("reserve");
        message.setAction(SagaTccAction.TRY);
        message.setRequestClass(request.getClass().getName());
        message.setRequestJson(requestJson);
        message.setAttempt(1);
        message.setMessageKey("saga-9-TRY-1");
        return objectMapper.writeValueAsString(message).getBytes(StandardCharsets.UTF_8).length;
    }

    private SagaTccContext contextWith(SagaTccRequest... requests) {
        SagaTccContext context = new SagaTccContext("saga", "order", "create-order", "student-1");
        for (SagaTccRequest request : requests) {
            context.addEnlistment(new SagaTccEnlistment("wallet", "reserve", request));
        }
        return context;
    }

    private static final class PayloadRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;

        private final String value;

        private PayloadRequest(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
