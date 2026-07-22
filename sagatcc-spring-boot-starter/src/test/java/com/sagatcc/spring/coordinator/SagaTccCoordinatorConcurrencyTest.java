package com.sagatcc.spring.coordinator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SagaTccCoordinatorConcurrencyTest {

    private static final int BRANCH_COUNT = 24;

    private CoordinatorTestRepository repository;
    private ObjectMapper objectMapper;
    private SagaTccCoordinator firstCoordinator;
    private SagaTccCoordinator secondCoordinator;

    @BeforeEach
    void setUp() {
        repository = new CoordinatorTestRepository();
        objectMapper = new ObjectMapper();
        SagaMessagePublisher publisher = mock(SagaMessagePublisher.class);
        when(publisher.commandTopic(anyString())).thenAnswer(invocation ->
                "command-" + invocation.getArgument(0));
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        firstCoordinator = new SagaTccCoordinator(repository, publisher, objectMapper, properties, "order");
        secondCoordinator = new SagaTccCoordinator(repository, publisher, objectMapper, properties, "order");
    }

    @Test
    void concurrentDuplicateResultsForTheSameBranchProduceOneStateTransitionAndOneCommand() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.setDispatchedAttempts(1L, 1, 0, 0);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (int i = 0; i < 32; i++) {
            workers.add(() -> firstCoordinator.handleResult(result(1L, SagaTccAction.TRY, true)));
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.COMMITTING));
        assertEquals(32, repository.transactionForUpdateLookups());
        assertEquals(1, repository.enqueuedCount());
        assertAllCommands(SagaTccAction.CONFIRM, 1);
    }

    @Test
    void conflictingConcurrentResultsForTheSameBranchChooseExactlyOneCompensationPath() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(1L, SagaTccBranchStatus.TRYING, 1);
        List<Runnable> workers = new ArrayList<Runnable>();
        workers.add(() -> firstCoordinator.handleResult(result(1L, SagaTccAction.TRY, true)));
        workers.add(() -> secondCoordinator.handleResult(result(1L, SagaTccAction.TRY, false)));

        runAtTheSameTime(workers);

        SagaTccTransactionStatus transactionStatus = repository.transactionStatus("saga");
        SagaTccBranchStatus branchStatus = repository.branchStatus(1L);
        assertTrue(transactionStatus == SagaTccTransactionStatus.COMMITTING
                || transactionStatus == SagaTccTransactionStatus.CANCELLING);
        if (transactionStatus == SagaTccTransactionStatus.COMMITTING) {
            assertEquals(SagaTccBranchStatus.CONFIRMING, branchStatus);
            assertAllCommands(SagaTccAction.CONFIRM, 1);
        } else {
            assertEquals(SagaTccBranchStatus.CANCELLING, branchStatus);
            assertAllCommands(SagaTccAction.CANCEL, 1);
        }
        assertEquals(1, repository.enqueuedCount());
    }

    @Test
    void concurrentDuplicateRetryableFailuresForOneAttemptScheduleOneTimeoutRetry() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.CONFIRMING);
        repository.setDispatchedAttempts(1L, 1, 2, 0);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (int i = 0; i < 32; i++) {
            workers.add(() -> {
                SagaTccResultMessage failure = result(1L, SagaTccAction.CONFIRM, false);
                failure.setAttempt(2);
                firstCoordinator.handleResult(failure);
            });
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(1L));
        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(1, repository.scheduledBranchRetries());
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void concurrentLastTryResultsScheduleConfirmForEveryBranchExactlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            repository.addBranch(branchId, "saga", SagaTccBranchStatus.TRYING);
            repository.setDispatchedAttempts(branchId, 1, 0, 0);
            final long id = branchId;
            workers.add(() -> coordinatorFor(id).handleResult(result(id, SagaTccAction.TRY, true)));
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccTransactionStatus.COMMITTING, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.COMMITTING));
        assertEquals(BRANCH_COUNT, repository.transactionForUpdateLookups());
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            assertEquals(SagaTccBranchStatus.CONFIRMING, repository.branchStatus(branchId));
        }
        assertAllCommands(SagaTccAction.CONFIRM, BRANCH_COUNT);
        assertEveryBranchHasExactlyOneCommand();
    }

    @Test
    void concurrentTryResultsWithOnePermanentFailureScheduleCancelAfterEveryTryCompletes() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            repository.addBranch(branchId, "saga", SagaTccBranchStatus.TRYING);
            repository.setDispatchedAttempts(branchId, 1, 0, 0);
            final long id = branchId;
            workers.add(() -> {
                SagaTccResultMessage tryResult = result(id, SagaTccAction.TRY, id != 1L);
                if (id == 1L) {
                    tryResult.setRetryable(false);
                    tryResult.setErrorMessage("rejected");
                }
                coordinatorFor(id).handleResult(tryResult);
            });
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccTransactionStatus.CANCELLING, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.CANCELLING));
        assertEquals(BRANCH_COUNT, repository.transactionForUpdateLookups());
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            assertEquals(SagaTccBranchStatus.CANCELLING, repository.branchStatus(branchId));
        }
        assertAllCommands(SagaTccAction.CANCEL, BRANCH_COUNT);
        assertEveryBranchHasExactlyOneCommand();
    }

    @Test
    void concurrentLastConfirmResultsCommitTheSagaExactlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.COMMITTING);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            repository.addBranch(branchId, "saga", SagaTccBranchStatus.CONFIRMING);
            repository.setDispatchedAttempts(branchId, 1, 1, 0);
            final long id = branchId;
            workers.add(() -> coordinatorFor(id).handleResult(result(id, SagaTccAction.CONFIRM, true)));
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccTransactionStatus.COMMITTED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.COMMITTED));
        assertEquals(BRANCH_COUNT, repository.transactionForUpdateLookups());
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            assertEquals(SagaTccBranchStatus.CONFIRMED, repository.branchStatus(branchId));
        }
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void concurrentLastCancelResultsCancelTheSagaExactlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.CANCELLING);
        List<Runnable> workers = new ArrayList<Runnable>();
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            repository.addBranch(branchId, "saga", SagaTccBranchStatus.CANCELLING);
            repository.setDispatchedAttempts(branchId, 1, 0, 1);
            final long id = branchId;
            workers.add(() -> coordinatorFor(id).handleResult(result(id, SagaTccAction.CANCEL, true)));
        }

        runAtTheSameTime(workers);

        assertEquals(SagaTccTransactionStatus.CANCELLED, repository.transactionStatus("saga"));
        assertEquals(1, repository.successfulTransitionsTo(SagaTccTransactionStatus.CANCELLED));
        assertEquals(BRANCH_COUNT, repository.transactionForUpdateLookups());
        for (long branchId = 1; branchId <= BRANCH_COUNT; branchId++) {
            assertEquals(SagaTccBranchStatus.CANCELLED, repository.branchStatus(branchId));
        }
        assertEquals(0, repository.enqueuedCount());
    }

    @Test
    void concurrentRecoveryWorkersClaimTheSameRetryAttemptOnlyOnce() throws Exception {
        repository.addTransaction("saga", "order", SagaTccTransactionStatus.TRYING);
        repository.addBranch(1L, "saga", SagaTccBranchStatus.TRYING);
        repository.markActionDispatched(1L, SagaTccBranchStatus.TRYING, 1);
        repository.synchronizeRetryScans(2);
        List<Runnable> workers = new ArrayList<Runnable>();
        workers.add(() -> firstCoordinator.recoverTimeoutBranches());
        workers.add(() -> secondCoordinator.recoverTimeoutBranches());

        runAtTheSameTime(workers);

        assertEquals(2, repository.attempts(1L, SagaTccBranchStatus.TRYING));
        assertEquals(1, repository.enqueuedCount());
        assertAllCommands(SagaTccAction.TRY, 1);
        SagaTccCommandMessage command = objectMapper.readValue(
                repository.enqueuedOutbox().get(0).getPayload(), SagaTccCommandMessage.class);
        assertEquals(2, command.getAttempt());
    }

    private SagaTccCoordinator coordinatorFor(long branchId) {
        return branchId % 2L == 0L ? firstCoordinator : secondCoordinator;
    }

    private SagaTccResultMessage result(long branchId, SagaTccAction action, boolean success) {
        SagaTccResultMessage result = new SagaTccResultMessage();
        result.setSagaId("saga");
        result.setBranchId(branchId);
        result.setCoordinatorApp("order");
        result.setTargetApp("wallet");
        result.setBusCode("reserve");
        result.setAction(action);
        result.setAttempt(1);
        result.setSuccess(success);
        result.setRetryable(true);
        result.setErrorMessage(success ? null : "retryable");
        return result;
    }

    private void assertAllCommands(SagaTccAction expectedAction, int expectedCount) throws Exception {
        List<SagaTccOutboxRecord> outbox = repository.enqueuedOutbox();
        assertEquals(expectedCount, outbox.size());
        for (SagaTccOutboxRecord record : outbox) {
            SagaTccCommandMessage command = objectMapper.readValue(record.getPayload(), SagaTccCommandMessage.class);
            assertEquals(expectedAction, command.getAction());
        }
    }

    private void assertEveryBranchHasExactlyOneCommand() {
        Set<Long> branchIds = new HashSet<Long>();
        for (SagaTccOutboxRecord record : repository.enqueuedOutbox()) {
            assertTrue(branchIds.add(record.getBranchId()), "duplicate command for branch " + record.getBranchId());
        }
        assertEquals(BRANCH_COUNT, branchIds.size());
    }

    private void runAtTheSameTime(List<Runnable> workers) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(workers.size());
        CountDownLatch ready = new CountDownLatch(workers.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<Future<?>>();
        try {
            for (Runnable worker : workers) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("concurrent start latch timed out");
                    }
                    worker.run();
                    return null;
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS), "workers were not ready");
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "executor did not terminate");
        }
    }
}
