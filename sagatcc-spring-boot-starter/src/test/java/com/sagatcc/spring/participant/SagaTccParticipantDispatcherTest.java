package com.sagatcc.spring.participant;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccFailureClassifier;
import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.core.api.SagaTccParticipant;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.idempotent.ParticipantLogRepository;
import com.sagatcc.spring.messaging.SagaMessagePublisher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SagaTccParticipantDispatcherTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final List<ExecutorService> executors = new ArrayList<ExecutorService>();

    @AfterEach
    void shutDownExecutors() {
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
        }
    }

    @Test
    void tryConfirmAndCancelAreRoutedToTheirMatchingCallbacks() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        Fixture fixture = fixture(participant, directRepository(), failure -> true, "wallet");

        fixture.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));
        fixture.dispatcher.dispatch(payload(command(SagaTccAction.CONFIRM, 2L)));
        fixture.dispatcher.dispatch(payload(command(SagaTccAction.CANCEL, 3L)));

        assertEquals(1, participant.tryCalls.get());
        assertEquals(1, participant.confirmCalls.get());
        assertEquals(1, participant.cancelCalls.get());
        assertEquals(3, fixture.transactionManager.commits.get());
        assertEquals(0, fixture.transactionManager.rollbacks.get());
        assertEquals(3, fixture.publisher.published.size());
        for (Published published : fixture.publisher.published) {
            SagaTccResultMessage result = OBJECT_MAPPER.readValue(published.payload, SagaTccResultMessage.class);
            assertTrue(result.isSuccess());
            assertFalse(result.isRetryable());
            assertEquals(SagaMessagePublisher.RESULT_TAG, published.tag);
            assertEquals(result.getMessageKey(), published.messageKey);
        }
    }

    @Test
    void commandForAnotherApplicationIsIgnoredWithoutDatabaseOrResultTraffic() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        AtomicInteger repositoryCalls = new AtomicInteger();
        ParticipantLogRepository repository = (localApp, command, businessCall) -> {
            repositoryCalls.incrementAndGet();
            businessCall.call();
        };
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
        command.setTargetApp("inventory");

        fixture.dispatcher.dispatch(payload(command));

        assertEquals(0, repositoryCalls.get());
        assertEquals(0, participant.tryCalls.get());
        assertEquals(0, fixture.transactionManager.commits.get());
        assertTrue(fixture.publisher.published.isEmpty());
    }

    @Test
    void preBoundDataSourceWithoutAnActualTransactionCannotBypassParticipantGuard() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        SagaTccParticipantRegistry registry = new SagaTccParticipantRegistry(
                Collections.<SagaTccParticipant>singletonList(participant));
        RecordingPublisher publisher = new RecordingPublisher();
        RecordingTransactionManager transactionManager = new RecordingTransactionManager();
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("wallet");
        DataSource dataSource = mock(DataSource.class);
        SagaTccParticipantDispatcher dispatcher = new SagaTccParticipantDispatcher(registry, directRepository(),
                publisher, OBJECT_MAPPER, failure -> true, properties,
                new TransactionTemplate(transactionManager), new MockEnvironment(), dataSource);
        TransactionSynchronizationManager.bindResource(dataSource, new Object());
        try {
            dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));
        } finally {
            TransactionSynchronizationManager.unbindResource(dataSource);
        }

        assertEquals(0, participant.tryCalls.get());
        assertEquals(0, transactionManager.commits.get());
        assertEquals(1, transactionManager.rollbacks.get());
        SagaTccResultMessage result = OBJECT_MAPPER.readValue(
                publisher.published.get(0).payload, SagaTccResultMessage.class);
        assertFalse(result.isSuccess());
        assertTrue(result.getErrorMessage().contains("does not manage the participant JDBC DataSource"));
    }

    @Test
    void activeTransactionBoundToAnotherDataSourceIsRejected() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        ParticipantLogRepository repository = (localApp, command, businessCall) -> {
            throw new AssertionError("wrong DataSource must be rejected before idempotency work");
        };
        RecordingPublisher publisher = new RecordingPublisher();
        RecordingTransactionManager transactionManager = new RecordingTransactionManager();
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("wallet");
        DataSource expectedDataSource = mock(DataSource.class);
        DataSource otherDataSource = mock(DataSource.class);
        SagaTccParticipantDispatcher dispatcher = new SagaTccParticipantDispatcher(
                new SagaTccParticipantRegistry(Collections.<SagaTccParticipant>singletonList(participant)),
                repository, publisher, OBJECT_MAPPER, failure -> true, properties,
                new TransactionTemplate(transactionManager), new MockEnvironment(), expectedDataSource);
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.bindResource(otherDataSource, new Object());
        try {
            dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));
        } finally {
            TransactionSynchronizationManager.unbindResource(otherDataSource);
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }

        assertEquals(0, participant.tryCalls.get());
        assertEquals(1, transactionManager.rollbacks.get());
        assertFalse(OBJECT_MAPPER.readValue(
                publisher.published.get(0).payload, SagaTccResultMessage.class).isSuccess());
    }

    @Test
    void activeTransactionOwningTheExpectedDataSourceExecutesAndPublishesSuccess() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        AtomicInteger repositoryCalls = new AtomicInteger();
        ParticipantLogRepository repository = (localApp, command, businessCall) -> {
            repositoryCalls.incrementAndGet();
            businessCall.call();
        };
        RecordingPublisher publisher = new RecordingPublisher();
        RecordingTransactionManager transactionManager = new RecordingTransactionManager();
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("wallet");
        DataSource dataSource = mock(DataSource.class);
        SagaTccParticipantDispatcher dispatcher = new SagaTccParticipantDispatcher(
                new SagaTccParticipantRegistry(Collections.<SagaTccParticipant>singletonList(participant)),
                repository, publisher, OBJECT_MAPPER, failure -> true, properties,
                new TransactionTemplate(transactionManager), new MockEnvironment(), dataSource);
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.bindResource(dataSource, new Object());
        try {
            dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));
        } finally {
            TransactionSynchronizationManager.unbindResource(dataSource);
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }

        assertEquals(1, repositoryCalls.get());
        assertEquals(1, participant.tryCalls.get());
        assertEquals(1, transactionManager.commits.get());
        assertTrue(OBJECT_MAPPER.readValue(
                publisher.published.get(0).payload, SagaTccResultMessage.class).isSuccess());
    }

    @Test
    void malformedEnvelopeJsonIsRejectedAndDoesNotPublishAResult() {
        Fixture fixture = fixture(new RecordingParticipant(), directRepository(), failure -> true, "wallet");

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> fixture.dispatcher.dispatch("{not-json"));

        assertTrue(failure.getMessage().contains("parse SagaTcc command failed"));
        assertTrue(fixture.publisher.published.isEmpty());
        assertEquals(0, fixture.transactionManager.commits.get());
    }

    @Test
    void invalidEnvelopeBoundariesAreRejectedBeforeStartingATransaction() throws Exception {
        Fixture fixture = fixture(new RecordingParticipant(), directRepository(), failure -> true, "wallet");
        List<InvalidCommand> invalid = new ArrayList<InvalidCommand>();
        invalid.add(new InvalidCommand("null branch", c -> c.setBranchId(null)));
        invalid.add(new InvalidCommand("zero branch", c -> c.setBranchId(0L)));
        invalid.add(new InvalidCommand("negative branch", c -> c.setBranchId(-1L)));
        invalid.add(new InvalidCommand("null action", c -> c.setAction(null)));
        invalid.add(new InvalidCommand("zero attempt", c -> c.setAttempt(0)));
        invalid.add(new InvalidCommand("negative attempt", c -> c.setAttempt(-1)));
        invalid.add(new InvalidCommand("blank message key", c -> c.setMessageKey("  ")));
        invalid.add(new InvalidCommand("message key too long", c -> c.setMessageKey(repeat('k', 161))));
        invalid.add(new InvalidCommand("blank saga id", c -> c.setSagaId("")));
        invalid.add(new InvalidCommand("saga id too long", c -> c.setSagaId(repeat('s', 65))));
        invalid.add(new InvalidCommand("blank coordinator", c -> c.setCoordinatorApp("\t")));
        invalid.add(new InvalidCommand("coordinator too long", c -> c.setCoordinatorApp(repeat('c', 129))));
        invalid.add(new InvalidCommand("coordinator colon", c -> c.setCoordinatorApp("order:blue")));
        invalid.add(new InvalidCommand("blank target", c -> c.setTargetApp(" ")));
        invalid.add(new InvalidCommand("target too long", c -> c.setTargetApp(repeat('t', 129))));
        invalid.add(new InvalidCommand("target colon", c -> c.setTargetApp("wallet:blue")));
        invalid.add(new InvalidCommand("blank business code", c -> c.setBusCode(" ")));
        invalid.add(new InvalidCommand("business code too long", c -> c.setBusCode(repeat('b', 129))));
        invalid.add(new InvalidCommand("blank request class", c -> c.setRequestClass(" ")));
        invalid.add(new InvalidCommand("request class too long", c -> c.setRequestClass(repeat('r', 513))));
        invalid.add(new InvalidCommand("null request json", c -> c.setRequestJson(null)));

        for (InvalidCommand testCase : invalid) {
            SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
            testCase.mutation.accept(command);
            SagaTccException failure = assertThrows(SagaTccException.class,
                    () -> fixture.dispatcher.dispatch(payload(command)), testCase.name);
            assertTrue(failure.getMessage().contains("invalid SagaTcc command")
                            || failure.getMessage().contains("must not be null")
                            || failure.getMessage().contains("must not contain ':'")
                            || failure.getMessage().contains("RocketMQ-safe characters"),
                    testCase.name + " produced: " + failure.getMessage());
        }

        assertEquals(0, fixture.transactionManager.commits.get());
        assertEquals(0, fixture.transactionManager.rollbacks.get());
        assertTrue(fixture.publisher.published.isEmpty());
    }

    @Test
    void inclusiveMaximumTextLengthsPassEnvelopeValidation() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        Fixture fixture = fixture(participant, directRepository(), failure -> true, "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
        command.setMessageKey(repeat('m', 160));
        command.setSagaId(repeat('s', 64));
        command.setCoordinatorApp(repeat('c', 128));

        fixture.dispatcher.dispatch(payload(command));

        SagaTccResultMessage result = fixture.singleResult();
        assertTrue(result.isSuccess());
        assertEquals(1, participant.tryCalls.get());

        Fixture longLocalFixture = fixtureWithRegistry(Collections.<SagaTccParticipant>emptyList(),
                directRepository(), failure -> true, repeat('t', 128));
        SagaTccCommandMessage longFields = command(SagaTccAction.TRY, 2L);
        longFields.setTargetApp(repeat('t', 128));
        longFields.setBusCode(repeat('b', 128));
        longFields.setRequestClass(repeat('r', 512));
        longLocalFixture.dispatcher.dispatch(payload(longFields));
        assertFalse(longLocalFixture.singleResult().isSuccess(),
                "valid maxima proceed to participant lookup rather than failing envelope validation");
    }

    @Test
    void missingParticipantPublishesAWellFormedRetryableFailure() throws Exception {
        Fixture fixture = fixtureWithRegistry(Collections.<SagaTccParticipant>emptyList(),
                directRepository(), failure -> true, "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 7L);

        fixture.dispatcher.dispatch(payload(command));

        SagaTccResultMessage result = fixture.singleResult();
        assertFailureIdentity(result, command);
        assertTrue(result.isRetryable());
        assertEquals(SagaTccException.class.getName(), result.getErrorCode());
        assertTrue(result.getErrorMessage().contains("no SagaTcc participant"));
        assertEquals(0, fixture.transactionManager.commits.get());
    }

    @Test
    void mismatchedRequestClassNeverDeserializesOrInvokesParticipant() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        Fixture fixture = fixture(participant, directRepository(), defaultClassifier(), "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
        command.setRequestClass("malicious.UnexpectedRequest");
        command.setRequestJson("{definitely-not-json");

        fixture.dispatcher.dispatch(payload(command));

        SagaTccResultMessage result = fixture.singleResult();
        assertFalse(result.isSuccess());
        assertFalse(result.isRetryable());
        assertTrue(result.getErrorMessage().contains("request class does not match"));
        assertEquals(0, participant.tryCalls.get());
        assertEquals(0, fixture.transactionManager.commits.get());
    }

    @Test
    void invalidRequestPayloadPublishesFailureWithoutOpeningATransaction() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        Fixture fixture = fixture(participant, directRepository(), defaultClassifier(), "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
        command.setRequestJson("{bad-request-json");

        fixture.dispatcher.dispatch(payload(command));

        SagaTccResultMessage result = fixture.singleResult();
        assertFalse(result.isSuccess());
        assertFalse(result.isRetryable());
        assertNotNull(result.getErrorCode());
        assertEquals(0, participant.tryCalls.get());
        assertEquals(0, fixture.transactionManager.commits.get());
        assertEquals(0, fixture.transactionManager.rollbacks.get());
    }

    @Test
    void requestUtf8LimitAcceptsExactBoundaryAndRejectsOneByteLessAsPermanent() throws Exception {
        SagaTccCommandMessage exactCommand = command(SagaTccAction.TRY, 1L);
        int requestBytes = exactCommand.getRequestJson().getBytes(StandardCharsets.UTF_8).length;
        SagaTccProperties exactProperties = new SagaTccProperties();
        exactProperties.setApplicationName("wallet");
        exactProperties.setMaxRequestBytes(requestBytes);
        RecordingParticipant accepted = new RecordingParticipant();
        Fixture exact = fixtureWithProperties(
                Collections.<SagaTccParticipant>singletonList(accepted), directRepository(),
                failure -> true, exactProperties);

        exact.dispatcher.dispatch(payload(exactCommand));

        assertEquals(1, accepted.tryCalls.get());
        assertTrue(exact.singleResult().isSuccess());

        SagaTccProperties tooSmallProperties = new SagaTccProperties();
        tooSmallProperties.setApplicationName("wallet");
        tooSmallProperties.setMaxRequestBytes(requestBytes - 1);
        RecordingParticipant rejected = new RecordingParticipant();
        Fixture tooSmall = fixtureWithProperties(
                Collections.<SagaTccParticipant>singletonList(rejected), directRepository(),
                failure -> true, tooSmallProperties);

        tooSmall.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 2L)));

        assertEquals(0, rejected.tryCalls.get());
        assertFalse(tooSmall.singleResult().isSuccess());
        assertFalse(tooSmall.singleResult().isRetryable());
        assertTrue(tooSmall.singleResult().getErrorMessage().contains("max-request-bytes"));
    }

    @Test
    void oversizedCommandEnvelopeFailsBeforeDatabaseOrResultTraffic() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);
        String commandPayload = payload(command);
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("wallet");
        properties.setMaxMessageBytes(commandPayload.getBytes(StandardCharsets.UTF_8).length - 1);
        Fixture fixture = fixtureWithProperties(
                Collections.<SagaTccParticipant>singletonList(new RecordingParticipant()), directRepository(),
                failure -> true, properties);

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> fixture.dispatcher.dispatch(commandPayload));

        assertTrue(failure.getMessage().contains("max-message-bytes"));
        assertEquals(0, fixture.transactionManager.commits.get());
        assertTrue(fixture.publisher.published.isEmpty());
    }

    @Test
    void permanentBusinessFailureRollsBackAndStopsCoordinatorRetries() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        participant.tryFailure = new PermanentBusinessException("student account is closed");
        Fixture fixture = fixture(participant, directRepository(),
                failure -> !(failure instanceof PermanentBusinessException), "wallet");
        SagaTccCommandMessage command = command(SagaTccAction.TRY, 1L);

        fixture.dispatcher.dispatch(payload(command));

        SagaTccResultMessage result = fixture.singleResult();
        assertFailureIdentity(result, command);
        assertFalse(result.isRetryable());
        assertEquals(PermanentBusinessException.class.getName(), result.getErrorCode());
        assertEquals("student account is closed", result.getErrorMessage());
        assertEquals(0, fixture.transactionManager.commits.get());
        assertEquals(1, fixture.transactionManager.rollbacks.get());
    }

    @Test
    void retryableBusinessFailureIsReportedForRecovery() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        participant.tryFailure = new TransientBusinessException("downstream timeout");
        Fixture fixture = fixture(participant, directRepository(),
                failure -> failure instanceof TransientBusinessException, "wallet");

        fixture.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));

        SagaTccResultMessage result = fixture.singleResult();
        assertFalse(result.isSuccess());
        assertTrue(result.isRetryable());
        assertEquals(TransientBusinessException.class.getName(), result.getErrorCode());
        assertEquals(1, fixture.transactionManager.rollbacks.get());
    }

    @Test
    void classifierFailureDefaultsToRetryableSoWorkIsNotSilentlyLost() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        participant.tryFailure = new PermanentBusinessException("failure");
        SagaTccFailureClassifier brokenClassifier = failure -> {
            throw new IllegalStateException("classifier unavailable");
        };
        Fixture fixture = fixture(participant, directRepository(), brokenClassifier, "wallet");

        fixture.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));

        assertTrue(fixture.singleResult().isRetryable());
    }

    @Test
    void nestedFailureReportsDeepestCauseAndTruncatesBrokerPayloadMessage() throws Exception {
        String oversized = repeat('x', 2200);
        ParticipantLogRepository repository = (localApp, command, call) -> {
            throw new SagaTccException("repository wrapper",
                    new TransientBusinessException(oversized));
        };
        Fixture fixture = fixture(new RecordingParticipant(), repository, failure -> true, "wallet");

        fixture.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L)));

        SagaTccResultMessage result = fixture.singleResult();
        assertEquals(TransientBusinessException.class.getName(), result.getErrorCode());
        assertEquals(2000, result.getErrorMessage().length());
        assertEquals(oversized.substring(0, 2000), result.getErrorMessage());
    }

    @Test
    void resultPublisherFailureIsSurfacedAfterBusinessCommit() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        Fixture fixture = fixture(participant, directRepository(), failure -> true, "wallet");
        fixture.publisher.failure = new IllegalStateException("broker unavailable");

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> fixture.dispatcher.dispatch(payload(command(SagaTccAction.TRY, 1L))));

        assertTrue(failure.getMessage().contains("publish SagaTcc result failed"));
        assertEquals(1, participant.tryCalls.get());
        assertEquals(1, fixture.transactionManager.commits.get());
        assertEquals(0, fixture.transactionManager.rollbacks.get());
    }

    @Test
    void failedFirstAttemptIsNotMistakenForACompletedDuplicate() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        participant.failFirstTry.set(true);
        InMemoryIdempotentRepository repository = new InMemoryIdempotentRepository();
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        String payload = payload(command(SagaTccAction.TRY, 1L));

        fixture.dispatcher.dispatch(payload);
        fixture.dispatcher.dispatch(payload);

        assertEquals(2, participant.tryCalls.get());
        assertFalse(fixture.result(0).isSuccess());
        assertTrue(fixture.result(1).isSuccess());
        assertEquals(1, fixture.transactionManager.commits.get());
        assertEquals(1, fixture.transactionManager.rollbacks.get());
    }

    @Test
    void retryAttemptsPublishDistinctCorrelatableResultKeys() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        InMemoryIdempotentRepository repository = new InMemoryIdempotentRepository();
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        SagaTccCommandMessage first = command(SagaTccAction.TRY, 1L);
        SagaTccCommandMessage retry = command(SagaTccAction.TRY, 1L);
        retry.setAttempt(2);
        retry.setMessageKey("saga-student-1-TRY-2");

        fixture.dispatcher.dispatch(payload(first));
        fixture.dispatcher.dispatch(payload(retry));

        assertEquals(1, participant.tryCalls.get(), "successful retry delivery remains business-idempotent");
        assertEquals(1, fixture.result(0).getAttempt());
        assertEquals(2, fixture.result(1).getAttempt());
        assertEquals("saga-student-1-TRY-1-result", fixture.publisher.published.get(0).messageKey);
        assertEquals("saga-student-1-TRY-2-result", fixture.publisher.published.get(1).messageKey);
    }

    @Test
    void equalSagaAndBranchIdsFromDifferentCoordinatorsRemainIndependent() throws Exception {
        RecordingParticipant participant = new RecordingParticipant();
        InMemoryIdempotentRepository repository = new InMemoryIdempotentRepository();
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        SagaTccCommandMessage order = command(SagaTccAction.TRY, 1L);
        SagaTccCommandMessage admission = command(SagaTccAction.TRY, 1L);
        admission.setCoordinatorApp("admission");

        fixture.dispatcher.dispatch(payload(order));
        fixture.dispatcher.dispatch(payload(admission));

        assertEquals(2, participant.tryCalls.get());
        assertEquals("result-order", fixture.publisher.published.get(0).topic);
        assertEquals("result-admission", fixture.publisher.published.get(1).topic);
    }

    @Test
    void concurrentDuplicateDeliveryInvokesBusinessOnceAndAcknowledgesEveryDelivery() throws Exception {
        int deliveries = 64;
        RecordingParticipant participant = new RecordingParticipant();
        participant.tryDelayMillis = 15L;
        InMemoryIdempotentRepository repository = new InMemoryIdempotentRepository();
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        String payload = payload(command(SagaTccAction.TRY, 1L));
        ExecutorService executor = executor(deliveries);
        CountDownLatch ready = new CountDownLatch(deliveries);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < deliveries; i++) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                await(start);
                fixture.dispatcher.dispatch(payload);
            }));
        }

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

        assertEquals(1, participant.tryCalls.get(), "row-level idempotency must fence concurrent duplicates");
        assertEquals(deliveries, fixture.publisher.published.size(),
                "each broker delivery receives a result even when its business callback is skipped");
        assertEquals(deliveries, fixture.transactionManager.commits.get());
        for (int i = 0; i < deliveries; i++) {
            assertTrue(fixture.result(i).isSuccess());
        }
    }

    @Test
    void unrelatedBranchesCanExecuteConcurrentlyWithoutAGlobalParticipantLock() throws Exception {
        int branches = 24;
        RecordingParticipant participant = new RecordingParticipant();
        participant.tryDelayMillis = 30L;
        InMemoryIdempotentRepository repository = new InMemoryIdempotentRepository();
        Fixture fixture = fixture(participant, repository, failure -> true, "wallet");
        ExecutorService executor = executor(12);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (long branch = 1; branch <= branches; branch++) {
            final String payload = payload(command(SagaTccAction.TRY, branch));
            futures.add(executor.submit(() -> {
                await(start);
                fixture.dispatcher.dispatch(payload);
            }));
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

        assertEquals(branches, participant.tryCalls.get());
        assertTrue(participant.maxActiveTry.get() > 1,
                "idempotency serialization should be scoped to a branch, not the whole participant");
        assertEquals(branches, fixture.publisher.published.size());
    }

    private ExecutorService executor(int threads) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        executors.add(executor);
        return executor;
    }

    private static ParticipantLogRepository directRepository() {
        return (localApp, command, businessCall) -> businessCall.call();
    }

    private static SagaTccFailureClassifier defaultClassifier() {
        return failure -> !(failure instanceof SagaTccNonRetryableException);
    }

    private static Fixture fixture(RecordingParticipant participant, ParticipantLogRepository repository,
                                   SagaTccFailureClassifier classifier, String applicationName) {
        return fixtureWithRegistry(Collections.<SagaTccParticipant>singletonList(participant), repository,
                classifier, applicationName);
    }

    private static Fixture fixtureWithRegistry(List<SagaTccParticipant> participants,
                                               ParticipantLogRepository repository,
                                               SagaTccFailureClassifier classifier,
                                               String applicationName) {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName(applicationName);
        return fixtureWithProperties(participants, repository, classifier, properties);
    }

    private static Fixture fixtureWithProperties(List<SagaTccParticipant> participants,
                                                 ParticipantLogRepository repository,
                                                 SagaTccFailureClassifier classifier,
                                                 SagaTccProperties properties) {
        SagaTccParticipantRegistry registry = new SagaTccParticipantRegistry(participants);
        RecordingPublisher publisher = new RecordingPublisher();
        RecordingTransactionManager transactionManager = new RecordingTransactionManager();
        SagaTccParticipantDispatcher dispatcher = new SagaTccParticipantDispatcher(registry, repository,
                publisher, OBJECT_MAPPER, classifier, properties, new TransactionTemplate(transactionManager),
                new MockEnvironment());
        return new Fixture(dispatcher, publisher, transactionManager);
    }

    private static SagaTccCommandMessage command(SagaTccAction action, long branchId) {
        SagaTccCommandMessage command = new SagaTccCommandMessage();
        command.setMessageKey("saga-student-" + branchId + "-" + action + "-1");
        command.setSagaId("saga-student");
        command.setBranchId(branchId);
        command.setCoordinatorApp("order");
        command.setTargetApp("wallet");
        command.setBusCode("pay");
        command.setAction(action);
        command.setRequestClass(StudentPaymentRequest.class.getName());
        command.setRequestJson("{\"studentId\":10001,\"amount\":25}");
        command.setAttempt(1);
        return command;
    }

    private static String payload(SagaTccCommandMessage command) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(command);
    }

    private static void assertFailureIdentity(SagaTccResultMessage result, SagaTccCommandMessage command) {
        assertFalse(result.isSuccess());
        assertEquals(command.getSagaId(), result.getSagaId());
        assertEquals(command.getBranchId(), result.getBranchId());
        assertEquals(command.getCoordinatorApp(), result.getCoordinatorApp());
        assertEquals(command.getTargetApp(), result.getTargetApp());
        assertEquals(command.getBusCode(), result.getBusCode());
        assertEquals(command.getAction(), result.getAction());
        assertEquals(command.getAttempt(), result.getAttempt());
        assertEquals(command.getSagaId() + "-" + command.getBranchId() + "-" + command.getAction()
                        + "-" + command.getAttempt() + "-result",
                result.getMessageKey());
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @SagaTccBusiness(appId = "wallet", busCode = "pay")
    public static class StudentPaymentRequest implements SagaTccRequest {

        private static final long serialVersionUID = 1L;

        private long studentId;
        private int amount;

        public long getStudentId() {
            return studentId;
        }

        public void setStudentId(long studentId) {
            this.studentId = studentId;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }
    }

    private static final class RecordingParticipant implements SagaTccParticipant<StudentPaymentRequest> {

        private final AtomicInteger tryCalls = new AtomicInteger();
        private final AtomicInteger confirmCalls = new AtomicInteger();
        private final AtomicInteger cancelCalls = new AtomicInteger();
        private final AtomicInteger activeTry = new AtomicInteger();
        private final AtomicInteger maxActiveTry = new AtomicInteger();
        private final AtomicBoolean failFirstTry = new AtomicBoolean();
        private volatile RuntimeException tryFailure;
        private volatile long tryDelayMillis;

        @Override
        public void sagaTry(StudentPaymentRequest request) {
            tryCalls.incrementAndGet();
            int active = activeTry.incrementAndGet();
            updateMax(maxActiveTry, active);
            try {
                if (failFirstTry.compareAndSet(true, false)) {
                    throw new TransientBusinessException("first attempt failed");
                }
                if (tryFailure != null) {
                    throw tryFailure;
                }
                if (tryDelayMillis > 0) {
                    try {
                        Thread.sleep(tryDelayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TransientBusinessException("interrupted");
                    }
                }
            } finally {
                activeTry.decrementAndGet();
            }
        }

        @Override
        public void sagaConfirm(StudentPaymentRequest request) {
            confirmCalls.incrementAndGet();
        }

        @Override
        public void sagaCancel(StudentPaymentRequest request) {
            cancelCalls.incrementAndGet();
        }

        private static void updateMax(AtomicInteger maximum, int candidate) {
            int current;
            do {
                current = maximum.get();
                if (candidate <= current) {
                    return;
                }
            } while (!maximum.compareAndSet(current, candidate));
        }
    }

    private static final class InMemoryIdempotentRepository implements ParticipantLogRepository {

        private final Map<String, Object> locks = new ConcurrentHashMap<String, Object>();
        private final Set<String> succeeded = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        @Override
        public void executeIdempotently(String localApp, SagaTccCommandMessage command,
                                        java.util.concurrent.Callable<Void> businessCall) throws Exception {
            String key = localApp + ':' + command.getCoordinatorApp() + ':' + command.getSagaId() + ':'
                    + command.getBranchId() + ':' + command.getAction();
            Object created = new Object();
            Object existing = locks.putIfAbsent(key, created);
            Object lock = existing == null ? created : existing;
            synchronized (lock) {
                if (succeeded.contains(key)) {
                    return;
                }
                businessCall.call();
                succeeded.add(key);
            }
        }
    }

    private static final class RecordingTransactionManager implements PlatformTransactionManager {

        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();

        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
            commits.incrementAndGet();
        }

        @Override
        public void rollback(TransactionStatus status) {
            rollbacks.incrementAndGet();
        }
    }

    private static final class RecordingPublisher implements SagaMessagePublisher {

        private final List<Published> published = new CopyOnWriteArrayList<Published>();
        private volatile RuntimeException failure;

        @Override
        public String commandTopic(String targetApp) {
            return "command-" + targetApp;
        }

        @Override
        public String resultTopic(String coordinatorApp) {
            return "result-" + coordinatorApp;
        }

        @Override
        public void publishRaw(String topic, String tag, String messageKey, String payload) {
            if (failure != null) {
                throw failure;
            }
            published.add(new Published(topic, tag, messageKey, payload));
        }
    }

    private static final class Published {

        private final String topic;
        private final String tag;
        private final String messageKey;
        private final String payload;

        private Published(String topic, String tag, String messageKey, String payload) {
            this.topic = topic;
            this.tag = tag;
            this.messageKey = messageKey;
            this.payload = payload;
        }
    }

    private static final class Fixture {

        private final SagaTccParticipantDispatcher dispatcher;
        private final RecordingPublisher publisher;
        private final RecordingTransactionManager transactionManager;

        private Fixture(SagaTccParticipantDispatcher dispatcher, RecordingPublisher publisher,
                        RecordingTransactionManager transactionManager) {
            this.dispatcher = dispatcher;
            this.publisher = publisher;
            this.transactionManager = transactionManager;
        }

        private SagaTccResultMessage singleResult() throws Exception {
            assertEquals(1, publisher.published.size());
            return result(0);
        }

        private SagaTccResultMessage result(int index) throws Exception {
            return OBJECT_MAPPER.readValue(publisher.published.get(index).payload, SagaTccResultMessage.class);
        }
    }

    private static final class InvalidCommand {

        private final String name;
        private final Consumer<SagaTccCommandMessage> mutation;

        private InvalidCommand(String name, Consumer<SagaTccCommandMessage> mutation) {
            this.name = name;
            this.mutation = mutation;
        }
    }

    private static final class PermanentBusinessException extends RuntimeException {

        private PermanentBusinessException(String message) {
            super(message);
        }
    }

    private static final class TransientBusinessException extends RuntimeException {

        private TransientBusinessException(String message) {
            super(message);
        }
    }
}
