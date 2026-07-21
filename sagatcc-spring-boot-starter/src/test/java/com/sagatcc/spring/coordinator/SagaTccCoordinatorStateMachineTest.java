package com.sagatcc.spring.coordinator;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorStateMachineTest {

    private CoordinatorTestRepository repository;
    private SagaMessagePublisher publisher;
    private ObjectMapper objectMapper;
    private SagaTccProperties properties;
    private SagaTccCoordinator coordinator;

    @BeforeEach
    void setUp() {
        repository = new CoordinatorTestRepository();
        publisher = mock(SagaMessagePublisher.class);
        when(publisher.commandTopic(anyString())).thenAnswer(invocation ->
                "command-" + invocation.getArgument(0));
        objectMapper = new ObjectMapper();
        properties = new SagaTccProperties();
        properties.setApplicationName("order");
        coordinator = new SagaTccCoordinator(repository, publisher, objectMapper, properties, "order");
    }

    @Test
    void confirmAndCancelArrivingBeforeTryCompletionAreIgnored() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);

        coordinator.handleResult(result(1L, SagaTccAction.CONFIRM, true, false));
        coordinator.handleResult(result(1L, SagaTccAction.CANCEL, true, false));

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void lateTryResultCannotRegressAConfirmingBranch() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 1, 0);

        coordinator.handleResult(result(1L, SagaTccAction.TRY, false, true));

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void duplicateTrySuccessSchedulesConfirmExactlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);
        SagaTccResultMessage success = result(1L, SagaTccAction.TRY, true, false);

        coordinator.handleResult(success);
        coordinator.handleResult(success);

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.COMMITTING));
        assertEquals(1, repository.enqueuedCount());
        assertEquals(SagaTccAction.CONFIRM, onlyCommand().getAction());
    }

    @Test
    void duplicateTryFailureSchedulesCancelExactlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);
        SagaTccResultMessage failure = result(1L, SagaTccAction.TRY, false, false);

        coordinator.handleResult(failure);
        coordinator.handleResult(failure);

        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.CANCELLING, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.CANCELLING));
        assertEquals(1, repository.enqueuedCount());
        assertEquals(SagaTccAction.CANCEL, onlyCommand().getAction());
    }

    @Test
    void duplicateRetryableTryFailureSchedulesOneRetryWithoutPrematureCompensation() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);
        SagaTccResultMessage failure = result(1L, SagaTccAction.TRY, false, true);

        coordinator.handleResult(failure);
        coordinator.handleResult(failure);

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(1, repository.scheduledBranchRetries());
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void duplicateConfirmSuccessCommitsExactlyOnce() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 1, 0);
        SagaTccResultMessage success = result(1L, SagaTccAction.CONFIRM, true, false);

        coordinator.handleResult(success);
        coordinator.handleResult(success);

        assertEquals(SagaTccBranchStatus.CONFIRMED, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.COMMITTED));
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void duplicateCancelSuccessCancelsExactlyOnce() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.CANCELLING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CANCELLING);
        repository.setDispatchedAttempts(1L, 1, 0, 1);
        SagaTccResultMessage success = result(1L, SagaTccAction.CANCEL, true, false);

        coordinator.handleResult(success);
        coordinator.handleResult(success);

        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.CANCELLED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.CANCELLED));
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void resultForAnotherCoordinatorOrForgedBranchCoordinatesIsIgnored() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);

        SagaTccResultMessage otherCoordinator = result(1L, SagaTccAction.TRY, true, false);
        otherCoordinator.setCoordinatorApp("another-order-service");
        coordinator.handleResult(otherCoordinator);

        SagaTccResultMessage wrongSaga = result(1L, SagaTccAction.TRY, true, false);
        wrongSaga.setSagaId("other-saga");
        coordinator.handleResult(wrongSaga);

        SagaTccResultMessage wrongTarget = result(1L, SagaTccAction.TRY, true, false);
        wrongTarget.setTargetApp("course");
        coordinator.handleResult(wrongTarget);

        SagaTccResultMessage wrongBusiness = result(1L, SagaTccAction.TRY, true, false);
        wrongBusiness.setBusCode("other-operation");
        coordinator.handleResult(wrongBusiness);

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void nullOrStructurallyIncompleteResultsAreIgnored() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);

        coordinator.handleResult(null);
        SagaTccResultMessage noSaga = result(1L, SagaTccAction.TRY, true, false);
        noSaga.setSagaId(null);
        coordinator.handleResult(noSaga);
        SagaTccResultMessage noBranch = result(1L, SagaTccAction.TRY, true, false);
        noBranch.setBranchId(null);
        coordinator.handleResult(noBranch);
        SagaTccResultMessage noAction = result(1L, SagaTccAction.TRY, true, false);
        noAction.setAction(null);
        coordinator.handleResult(noAction);

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(0, repository.transactionForUpdateLookups());
    }

    @Test
    void duplicateRetryableFailureForTheSameAttemptSchedulesOnlyOneRetry() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 2, 0);
        SagaTccResultMessage failure = result(1L, SagaTccAction.CONFIRM, false, true);
        failure.setAttempt(2);

        coordinator.handleResult(failure);
        coordinator.handleResult(failure);

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(1, repository.scheduledBranchRetries());
    }

    @Test
    void staleFailureIsIgnoredButStaleSuccessStillCompletesTheAction() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 2, 0);
        SagaTccResultMessage staleFailure = result(1L, SagaTccAction.CONFIRM, false, true);
        staleFailure.setAttempt(1);

        coordinator.handleResult(staleFailure);

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(0, repository.scheduledBranchRetries());

        SagaTccResultMessage staleSuccess = result(1L, SagaTccAction.CONFIRM, true, false);
        staleSuccess.setAttempt(1);
        coordinator.handleResult(staleSuccess);

        assertEquals(SagaTccBranchStatus.CONFIRMED, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTED, repository.transactionStatus("saga"));
    }

    @Test
    void resultForAnAttemptThatWasNeverDispatchedIsIgnored() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 2, 0);
        SagaTccResultMessage futureFailure = result(1L, SagaTccAction.CONFIRM, false, true);
        futureFailure.setAttempt(3);
        SagaTccResultMessage futureSuccess = result(1L, SagaTccAction.CONFIRM, true, false);
        futureSuccess.setAttempt(3);

        coordinator.handleResult(futureFailure);
        coordinator.handleResult(futureSuccess);

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(0, repository.scheduledBranchRetries());
    }

    @Test
    void recoveryAtTheAttemptLimitStartsCompensationForEveryPotentialReservation() throws Exception {
        properties.setMaxAttempts(3);
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(1L, SagaTccBranchStatus.TRYING, 3);
        repository.addBranch(2L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(2L, SagaTccBranchStatus.TRYING, 1);
        repository.addBranch(3L, "saga", SagaTccBranchStatus.TRY_SUCCEEDED);

        coordinator.recoverTimeoutBranches();

        assertEquals(SagaTccTransactionStatus.CANCELLING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(3L));
        assertEquals(3, repository.enqueuedCount());
        for (SagaTccOutboxRecord outbox : repository.enqueuedOutbox()) {
            assertEquals(SagaTccAction.CANCEL,
                    objectMapper.readValue(outbox.getPayload(), SagaTccCommandMessage.class).getAction());
        }
    }

    @Test
    void recoveryAtOneBelowTheLimitDispatchesTheLastAllowedAttempt() throws Exception {
        properties.setMaxAttempts(3);
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(1L, SagaTccBranchStatus.TRYING, 2);

        coordinator.recoverTimeoutBranches();

        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(3, repository.attempts(1L, SagaTccBranchStatus.TRYING));
        assertEquals(1, repository.enqueuedCount());
        assertEquals(3, onlyCommand().getAttempt());
        assertEquals(SagaTccAction.TRY, onlyCommand().getAction());
    }

    @ParameterizedTest
    @EnumSource(value = SagaTccAction.class, names = {"CONFIRM", "CANCEL"})
    void exhaustedTerminalActionFailsTheSagaWithoutDispatchingAnotherAttempt(SagaTccAction action) {
        properties.setMaxAttempts(3);
        SagaTccTransactionStatus transactionStatus = action == SagaTccAction.CONFIRM
                ? SagaTccTransactionStatus.COMMITTING : SagaTccTransactionStatus.CANCELLING;
        SagaTccBranchStatus branchStatus = action == SagaTccAction.CONFIRM
                ? SagaTccBranchStatus.CONFIRMING : SagaTccBranchStatus.CANCELLING;
        repository.addTransaction("saga", "order", transactionStatus);
        repository.addBranch(1L, "saga", branchStatus);
        repository.markActionDispatched(1L, branchStatus, 3);

        coordinator.recoverTimeoutBranches();

        assertEquals(SagaTccBranchStatus.FAILED, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.FAILED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.FAILED));
        assertEquals(0, repository.enqueuedCount());
    }

    @ParameterizedTest
    @EnumSource(value = SagaTccAction.class, names = {"CONFIRM", "CANCEL"})
    void permanentFailureDoesNotStopOtherBranchesInTheDecidedPhase(SagaTccAction action) {
        SagaTccTransactionStatus transactionStatus = action == SagaTccAction.CONFIRM
                ? SagaTccTransactionStatus.COMMITTING : SagaTccTransactionStatus.CANCELLING;
        SagaTccBranchStatus branchStatus = action == SagaTccAction.CONFIRM
                ? SagaTccBranchStatus.CONFIRMING : SagaTccBranchStatus.CANCELLING;
        SagaTccBranchStatus completedStatus = action == SagaTccAction.CONFIRM
                ? SagaTccBranchStatus.CONFIRMED : SagaTccBranchStatus.CANCELLED;
        repository.addTransaction("saga", "order", transactionStatus);
        repository.addBranch(1L, "saga", branchStatus);
        repository.addBranch(2L, "saga", branchStatus);
        if (action == SagaTccAction.CONFIRM) {
            repository.setDispatchedAttempts(1L, 1, 1, 0);
            repository.setDispatchedAttempts(2L, 1, 1, 0);
        } else {
            repository.setDispatchedAttempts(1L, 1, 0, 1);
            repository.setDispatchedAttempts(2L, 1, 0, 1);
        }

        coordinator.handleResult(result(1L, action, false, false));

        assertEquals(SagaTccBranchStatus.FAILED, repository.branchStatus(1L));
        assertEquals(branchStatus, repository.branchStatus(2L));
        assertEquals(transactionStatus, repository.transactionStatus("saga"));

        coordinator.handleResult(result(2L, action, true, false));

        assertEquals(completedStatus, repository.branchStatus(2L));
        assertEquals(SagaTccTransactionStatus.FAILED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.FAILED));
    }

    @Test
    void commandStillWaitingForBrokerDeliveryDoesNotConsumeABusinessAttempt() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(1L, SagaTccBranchStatus.TRYING, 1);
        repository.markCurrentCommandPending(1L);

        coordinator.recoverTimeoutBranches();

        assertEquals(1, repository.attempts(1L, SagaTccBranchStatus.TRYING));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(0, repository.enqueuedCount());
    }

    private SagaTccCommandMessage onlyCommand() throws Exception {
        List<SagaTccOutboxRecord> outbox = repository.enqueuedOutbox();
        assertEquals(1, outbox.size());
        return objectMapper.readValue(outbox.get(0).getPayload(), SagaTccCommandMessage.class);
    }

    private SagaTccResultMessage result(long branchId, SagaTccAction action, boolean success, boolean retryable) {
        SagaTccResultMessage result = new SagaTccResultMessage();
        result.setSagaId("saga");
        result.setBranchId(branchId);
        result.setCoordinatorApp("order");
        result.setTargetApp("wallet");
        result.setBusCode("reserve");
        result.setAction(action);
        result.setAttempt(1);
        result.setSuccess(success);
        result.setRetryable(retryable);
        return result;
    }
}
