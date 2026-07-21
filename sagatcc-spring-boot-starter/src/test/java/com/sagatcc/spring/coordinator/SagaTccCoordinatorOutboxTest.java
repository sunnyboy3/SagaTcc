package com.sagatcc.spring.coordinator;

import java.util.Arrays;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.store.SagaTccRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorOutboxTest {

    private SagaTccRepository repository;
    private SagaMessagePublisher publisher;
    private SagaTccProperties properties;
    private SagaTccCoordinator coordinator;

    @BeforeEach
    void setUp() {
        repository = mock(SagaTccRepository.class);
        publisher = mock(SagaMessagePublisher.class);
        properties = new SagaTccProperties();
        properties.setScanBatchSize(5);
        properties.setOutboxClaimBatchSize(3);
        coordinator = new SagaTccCoordinator(repository, publisher, new ObjectMapper(), properties, "order");
    }

    @Test
    void successfullyPublishedClaimedRowsAreAcknowledgedWithTheirClaimTokens() {
        SagaTccOutboxRecord first = outbox(1L, "claim-1", 0);
        SagaTccOutboxRecord second = outbox(2L, "claim-2", 2);
        when(repository.claimReadyOutbox(3)).thenReturn(Arrays.asList(first, second));

        coordinator.publishPendingOutbox();

        InOrder ordered = inOrder(publisher, repository);
        ordered.verify(publisher).publishRaw(first.getTopic(), first.getTag(), first.getMessageKey(), first.getPayload());
        ordered.verify(repository).markOutboxSent(first);
        ordered.verify(publisher).publishRaw(second.getTopic(), second.getTag(), second.getMessageKey(), second.getPayload());
        ordered.verify(repository).markOutboxSent(second);
        verify(repository, never()).markOutboxFailed(any(SagaTccOutboxRecord.class), anyInt());
        verify(repository, never()).claimNextReadyOutbox();
    }

    @Test
    void onePublishFailureIsReleasedForRetryAndDoesNotBlockTheRestOfTheClaimedBatch() {
        SagaTccOutboxRecord failed = outbox(1L, "claim-failed", 3);
        SagaTccOutboxRecord succeeded = outbox(2L, "claim-success", 0);
        when(repository.claimReadyOutbox(3)).thenReturn(Arrays.asList(failed, succeeded));
        doThrow(new RuntimeException("broker unavailable")).when(publisher).publishRaw(
                failed.getTopic(), failed.getTag(), failed.getMessageKey(), failed.getPayload());

        coordinator.publishPendingOutbox();

        verify(repository).markOutboxFailed(failed, 4);
        verify(repository, never()).markOutboxSent(failed);
        verify(repository).markOutboxSent(succeeded);
        verify(publisher).publishRaw(succeeded.getTopic(), succeeded.getTag(),
                succeeded.getMessageKey(), succeeded.getPayload());
    }

    @Test
    void scanLimitIsHonouredAcrossMultipleSetBasedClaimsIncludingTheTailBatch() {
        properties.setScanBatchSize(5);
        properties.setOutboxClaimBatchSize(2);
        SagaTccOutboxRecord first = outbox(1L, "claim-1", 0);
        SagaTccOutboxRecord second = outbox(2L, "claim-2", 0);
        SagaTccOutboxRecord third = outbox(3L, "claim-3", 0);
        SagaTccOutboxRecord fourth = outbox(4L, "claim-4", 0);
        SagaTccOutboxRecord fifth = outbox(5L, "claim-5", 0);
        when(repository.claimReadyOutbox(2))
                .thenReturn(Arrays.asList(first, second))
                .thenReturn(Arrays.asList(third, fourth));
        when(repository.claimReadyOutbox(1)).thenReturn(Collections.singletonList(fifth));

        coordinator.publishPendingOutbox();

        verify(repository, times(2)).claimReadyOutbox(2);
        verify(repository).claimReadyOutbox(1);
        verify(repository, times(5)).markOutboxSent(any(SagaTccOutboxRecord.class));
        verify(publisher, times(5)).publishRaw(any(String.class), any(String.class), any(String.class), any(String.class));
    }

    @Test
    void emptyClaimReturnsWithoutCallingTheBroker() {
        when(repository.claimReadyOutbox(3)).thenReturn(Collections.emptyList());

        coordinator.publishPendingOutbox();

        verify(repository).claimReadyOutbox(3);
        verifyNoInteractions(publisher);
        verify(repository, never()).markOutboxSent(any(SagaTccOutboxRecord.class));
        verify(repository, never()).markOutboxFailed(any(SagaTccOutboxRecord.class), anyInt());
    }

    private SagaTccOutboxRecord outbox(long id, String claimToken, int attempts) {
        SagaTccOutboxRecord outbox = new SagaTccOutboxRecord();
        outbox.setId(id);
        outbox.setTopic("command-topic");
        outbox.setTag("SAGA_TCC_COMMAND");
        outbox.setMessageKey("message-" + id);
        outbox.setPayload("payload-" + id);
        outbox.setAttempts(attempts);
        outbox.setClaimToken(claimToken);
        return outbox;
    }
}
