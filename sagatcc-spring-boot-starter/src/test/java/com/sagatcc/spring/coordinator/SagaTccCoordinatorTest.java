package com.sagatcc.spring.coordinator;

import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.store.SagaTccRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorTest {

    private SagaTccRepository repository;
    private SagaMessagePublisher publisher;
    private ObjectMapper objectMapper;
    private SagaTccCoordinator coordinator;

    @BeforeEach
    void setUp() {
        repository = mock(SagaTccRepository.class);
        publisher = mock(SagaMessagePublisher.class);
        objectMapper = new ObjectMapper();
        SagaTccProperties properties = new SagaTccProperties();
        coordinator = new SagaTccCoordinator(repository, publisher, objectMapper, properties);
        SagaTccTransactionRecord lockedTransaction = new SagaTccTransactionRecord();
        lockedTransaction.setSagaId("saga");
        lockedTransaction.setStatus(SagaTccTransactionStatus.COMMITTING);
        when(repository.findTransactionForUpdate("saga")).thenReturn(lockedTransaction);
    }

    @Test
    void recoveryAdvancesAttemptAndCreatesANewCommand() throws Exception {
        SagaTccBranchRecord branch = branch(SagaTccBranchStatus.TRYING);
        branch.setTryAttempts(1);
        SagaTccTransactionRecord transaction = new SagaTccTransactionRecord();
        transaction.setSagaId("saga");
        transaction.setCoordinatorApp("order");
        transaction.setStatus(SagaTccTransactionStatus.TRYING);

        when(repository.findRetryableBranches()).thenReturn(Collections.singletonList(branch));
        when(repository.findTransaction("saga")).thenReturn(transaction);
        when(repository.markRetryDispatched(9L, SagaTccBranchStatus.TRYING, 1, 2,
                SagaTccTransactionStatus.TRYING)).thenReturn(true);
        when(publisher.commandTopic("wallet")).thenReturn("command");

        coordinator.recoverTimeoutBranches();

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(repository).enqueueOutbox(eq("saga-9-TRY-2"), eq("saga"), eq(9L), eq("command"),
                eq(SagaMessagePublisher.COMMAND_TAG), eq(SagaTccAction.TRY), eq(2), payload.capture());
        SagaTccCommandMessage command = objectMapper.readValue(payload.getValue(), SagaTccCommandMessage.class);
        assertEquals(2, command.getAttempt());
    }

    @Test
    void staleTryResultCannotRegressBranchState() {
        SagaTccBranchRecord branch = branch(SagaTccBranchStatus.CONFIRMING);
        when(repository.findBranch(9L)).thenReturn(branch);
        when(repository.transitionBranchStatus(9L, SagaTccBranchStatus.TRYING,
                SagaTccBranchStatus.TRY_FAILED, "late")).thenReturn(false);

        SagaTccResultMessage result = result(SagaTccAction.TRY, false, true);
        result.setErrorMessage("late");
        coordinator.handleResult(result);

        verify(repository, never()).findBranches("saga");
        verify(repository, never()).updateTransactionStatus(any(String.class), any(SagaTccTransactionStatus.class));
    }

    @Test
    void permanentConfirmFailureStopsRetrying() {
        SagaTccBranchRecord branch = branch(SagaTccBranchStatus.CONFIRMING);
        when(repository.findBranch(9L)).thenReturn(branch);
        when(repository.transitionBranchStatus(9L, SagaTccBranchStatus.CONFIRMING,
                SagaTccBranchStatus.FAILED, "rejected")).thenReturn(true);

        SagaTccResultMessage result = result(SagaTccAction.CONFIRM, false, false);
        result.setErrorMessage("rejected");
        coordinator.handleResult(result);

        verify(repository).completeTransactionPhase("saga", SagaTccTransactionStatus.COMMITTING,
                SagaTccTransactionStatus.COMMITTED, SagaTccBranchStatus.CONFIRMED);
        verify(repository, never()).scheduleBranchRetry(anyLong(), anyInt());
    }

    @Test
    void publisherSendsOnlyClaimedOutboxRecords() {
        SagaTccOutboxRecord outbox = new SagaTccOutboxRecord();
        outbox.setId(3L);
        outbox.setTopic("topic");
        outbox.setTag("tag");
        outbox.setMessageKey("key");
        outbox.setPayload("payload");
        outbox.setClaimToken("claim");
        when(repository.claimReadyOutbox(anyInt())).thenReturn(Collections.singletonList(outbox));

        coordinator.publishPendingOutbox();

        verify(publisher).publishRaw("topic", "tag", "key", "payload");
        verify(repository).markOutboxSent(outbox);
    }

    @Test
    void sharedResultTopicIgnoresOtherCoordinatorApplications() {
        SagaTccCoordinator applicationCoordinator = new SagaTccCoordinator(repository, publisher, objectMapper,
                new SagaTccProperties(), "order");
        SagaTccResultMessage result = result(SagaTccAction.TRY, true, false);
        result.setCoordinatorApp("another-order-service");

        applicationCoordinator.handleResult(result);

        verify(repository, never()).findBranch(anyLong());
    }

    private SagaTccBranchRecord branch(SagaTccBranchStatus status) {
        SagaTccBranchRecord branch = new SagaTccBranchRecord();
        branch.setId(9L);
        branch.setSagaId("saga");
        branch.setTargetApp("wallet");
        branch.setBusCode("pay");
        branch.setRequestClass("example.PayRequest");
        branch.setRequestJson("{}");
        branch.setStatus(status);
        branch.setTryAttempts(1);
        branch.setConfirmAttempts(1);
        branch.setCancelAttempts(1);
        return branch;
    }

    private SagaTccResultMessage result(SagaTccAction action, boolean success, boolean retryable) {
        SagaTccResultMessage result = new SagaTccResultMessage();
        result.setSagaId("saga");
        result.setBranchId(9L);
        result.setTargetApp("wallet");
        result.setBusCode("pay");
        result.setAction(action);
        result.setAttempt(1);
        result.setSuccess(success);
        result.setRetryable(retryable);
        return result;
    }
}
