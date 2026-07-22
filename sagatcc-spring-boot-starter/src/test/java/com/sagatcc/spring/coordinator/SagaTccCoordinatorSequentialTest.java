package com.sagatcc.spring.coordinator;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccEnlistment;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccBranchExecutionMode;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorSequentialTest {

    private CoordinatorTestRepository repository;
    private ObjectMapper objectMapper;
    private SagaTccProperties properties;
    private SagaTccCoordinator coordinator;

    @BeforeEach
    void setUp() {
        repository = new CoordinatorTestRepository();
        objectMapper = new ObjectMapper();
        properties = new SagaTccProperties();
        properties.setBranchExecutionMode(SagaTccBranchExecutionMode.SEQUENTIAL);
        SagaMessagePublisher publisher = mock(SagaMessagePublisher.class);
        when(publisher.commandTopic(anyString())).thenAnswer(invocation ->
                "sagatcc-command-" + invocation.getArgument(0));
        coordinator = new SagaTccCoordinator(repository, publisher, objectMapper, properties, "order");
    }

    @Test
    void onlyFirstTryIsDispatchedWhenSagaIsPersisted() throws Exception {
        SagaTccContext context = contextWithThreeBranches();

        coordinator.persistAndScheduleTry(context);

        assertEquals(SagaTccTransactionStatus.TRYING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(101L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(102L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(103L));
        assertCommand(0, 101L, SagaTccAction.TRY);
        assertEquals(1, repository.enqueuedCount());
    }

    @Test
    void sameCoordinatorSupportsSequentialAndParallelBusinessTransactions() {
        SagaTccProperties mixedProperties = new SagaTccProperties();
        mixedProperties.getBranchExecutionModes().put(
                "create-order", SagaTccBranchExecutionMode.SEQUENTIAL);
        SagaMessagePublisher publisher = mock(SagaMessagePublisher.class);
        when(publisher.commandTopic(anyString())).thenAnswer(invocation ->
                "sagatcc-command-" + invocation.getArgument(0));
        SagaTccCoordinator mixedCoordinator = new SagaTccCoordinator(
                repository, publisher, objectMapper, mixedProperties, "order");

        mixedCoordinator.persistAndScheduleTry(
                contextWithThreeBranches("sequential-saga", "create-order"));
        mixedCoordinator.persistAndScheduleTry(
                contextWithThreeBranches("parallel-saga", "bulk-order"));

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(101L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(102L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(103L));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(104L));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(105L));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(106L));
        assertEquals(4, repository.enqueuedCount());

        SagaTccResultMessage firstSequentialResult = result(101L, SagaTccAction.TRY, true, false);
        firstSequentialResult.setSagaId("sequential-saga");
        mixedCoordinator.handleResult(firstSequentialResult);

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(102L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(103L));
        assertEquals(5, repository.enqueuedCount());
    }

    @Test
    void tryAndConfirmAdvanceStrictlyInRegistrationOrder() throws Exception {
        addTryBranches(SagaTccBranchStatus.TRYING,
                SagaTccBranchStatus.NEW, SagaTccBranchStatus.NEW);

        coordinator.handleResult(result(1L, SagaTccAction.TRY, true, false));
        assertEquals(SagaTccBranchStatus.TRY_SUCCEEDED, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(3L));

        coordinator.handleResult(result(2L, SagaTccAction.TRY, true, false));
        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(3L));

        coordinator.handleResult(result(3L, SagaTccAction.TRY, true, false));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.TRY_SUCCEEDED, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.TRY_SUCCEEDED, repository.branchStatus(3L));

        coordinator.handleResult(result(1L, SagaTccAction.CONFIRM, true, false));
        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(2L));
        coordinator.handleResult(result(2L, SagaTccAction.CONFIRM, true, false));
        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(3L));
        coordinator.handleResult(result(3L, SagaTccAction.CONFIRM, true, false));

        assertEquals(SagaTccTransactionStatus.COMMITTED, repository.transactionStatus("saga"));
        assertCommand(0, 2L, SagaTccAction.TRY);
        assertCommand(1, 3L, SagaTccAction.TRY);
        assertCommand(2, 1L, SagaTccAction.CONFIRM);
        assertCommand(3, 2L, SagaTccAction.CONFIRM);
        assertCommand(4, 3L, SagaTccAction.CONFIRM);
        assertEquals(5, repository.enqueuedCount());
    }

    @Test
    void tryFailureSkipsUnstartedBranchesAndCancelsInReverseOrder() throws Exception {
        addTryBranches(SagaTccBranchStatus.TRY_SUCCEEDED,
                SagaTccBranchStatus.TRYING, SagaTccBranchStatus.NEW);

        SagaTccResultMessage failure = result(2L, SagaTccAction.TRY, false, false);
        failure.setErrorMessage("余额不足");
        coordinator.handleResult(failure);

        assertEquals(SagaTccTransactionStatus.CANCELLING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.TRY_SUCCEEDED, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(3L));
        assertCommand(0, 2L, SagaTccAction.CANCEL);

        coordinator.handleResult(result(2L, SagaTccAction.CANCEL, true, false));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(1L));
        assertCommand(1, 1L, SagaTccAction.CANCEL);

        coordinator.handleResult(result(1L, SagaTccAction.CANCEL, true, false));
        assertEquals(SagaTccTransactionStatus.CANCELLED, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(3L));
    }

    @Test
    void retryableFailureDoesNotAdvanceToNextBranch() {
        addTryBranches(SagaTccBranchStatus.TRYING,
                SagaTccBranchStatus.NEW, SagaTccBranchStatus.NEW);

        SagaTccResultMessage failure = result(1L, SagaTccAction.TRY, false, true);
        failure.setErrorMessage("服务暂时不可用");
        coordinator.handleResult(failure);

        assertEquals(SagaTccBranchStatus.TRYING, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.NEW, repository.branchStatus(3L));
        assertEquals(1, repository.scheduledBranchRetries());
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void exhaustedTryStartsReverseCompensationWithoutExecutingLaterBranches() throws Exception {
        properties.setMaxAttempts(1);
        addTryBranches(SagaTccBranchStatus.TRYING,
                SagaTccBranchStatus.NEW, SagaTccBranchStatus.NEW);

        coordinator.recoverTimeoutBranches();

        assertEquals(SagaTccTransactionStatus.CANCELLING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(3L));
        assertCommand(0, 1L, SagaTccAction.CANCEL);
    }

    @Test
    void permanentConfirmFailureStopsLaterBranchesAndFailsSaga() {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.addBranch(2L, "saga", SagaTccBranchStatus.TRY_SUCCEEDED);
        repository.addBranch(3L, "saga", SagaTccBranchStatus.TRY_SUCCEEDED);
        repository.setDispatchedAttempts(1L, 1, 1, 0);

        SagaTccResultMessage failure = result(1L, SagaTccAction.CONFIRM, false, false);
        failure.setErrorMessage("确认被拒绝");
        coordinator.handleResult(failure);

        assertEquals(SagaTccTransactionStatus.FAILED, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.FAILED, repository.branchStatus(1L));
        assertEquals(SagaTccBranchStatus.FAILED, repository.branchStatus(2L));
        assertEquals(SagaTccBranchStatus.FAILED, repository.branchStatus(3L));
        assertEquals(0, repository.enqueuedCount());
    }

    private SagaTccContext contextWithThreeBranches() {
        return contextWithThreeBranches("saga", "create-order");
    }

    private SagaTccContext contextWithThreeBranches(String sagaId, String businessCode) {
        SagaTccContext context = new SagaTccContext(sagaId, "order", businessCode, "1001");
        context.addEnlistment(new SagaTccEnlistment("wallet", "reserve", new TestRequest("wallet")));
        context.addEnlistment(new SagaTccEnlistment("inventory", "reserve", new TestRequest("inventory")));
        context.addEnlistment(new SagaTccEnlistment("order", "finalize", new TestRequest("order")));
        return context;
    }

    private void addTryBranches(SagaTccBranchStatus first, SagaTccBranchStatus second,
                                SagaTccBranchStatus third) {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", first);
        repository.addBranch(2L, "saga", second);
        repository.addBranch(3L, "saga", third);
        repository.setDispatchedAttempts(1L, 1, 0, 0);
        repository.setDispatchedAttempts(2L, second == SagaTccBranchStatus.TRYING ? 1 : 0, 0, 0);
    }

    private SagaTccResultMessage result(long branchId, SagaTccAction action,
                                        boolean success, boolean retryable) {
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

    private void assertCommand(int index, long branchId, SagaTccAction action) throws Exception {
        List<SagaTccOutboxRecord> outbox = repository.enqueuedOutbox();
        assertTrue(outbox.size() > index);
        SagaTccCommandMessage command = objectMapper.readValue(outbox.get(index).getPayload(),
                SagaTccCommandMessage.class);
        assertEquals(branchId, command.getBranchId().longValue());
        assertEquals(action, command.getAction());
    }

    private static final class TestRequest implements SagaTccRequest {

        private static final long serialVersionUID = 1L;

        private final String value;

        private TestRequest(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
