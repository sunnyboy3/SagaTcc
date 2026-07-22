package com.sagatcc.spring.idempotent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.spring.config.SagaTccProperties;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class JdbcParticipantLogRepositoryIntegrationTest {

    private JdbcTemplate jdbc;
    private JdbcParticipantLogRepository repository;
    private TransactionTemplate transaction;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:participant_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=30000");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        jdbc = new JdbcTemplate(dataSource);
        createParticipantLogTable("");
        repository = new JdbcParticipantLogRepository(jdbc);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    private void createParticipantLogTable(String prefix) {
        jdbc.execute("create table " + prefix + "saga_tcc_participant_log ("
                + "id bigint auto_increment primary key, local_app varchar(128) not null, "
                + "coordinator_app varchar(128) not null, saga_id varchar(64) not null, "
                + "branch_id bigint not null, target_app varchar(128) not null, bus_code varchar(128) not null, "
                + "request_hash varchar(64) not null, try_status varchar(32), confirm_status varchar(32), "
                + "cancel_status varchar(32), create_time timestamp(3) not null, update_time timestamp(3) not null, "
                + "unique (local_app, coordinator_app, saga_id, branch_id))");
    }

    @Test
    void sixtyFourConcurrentDuplicatesExecuteTryExactlyOnce() throws Exception {
        int deliveries = 64;
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger businessCalls = new AtomicInteger();
        SagaTccCommandMessage command = command("same-saga", 7L, SagaTccAction.TRY, "coordinator");
        List<Future<?>> futures = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < deliveries; i++) {
                futures.add(executor.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    execute(command, () -> {
                        businessCalls.incrementAndGet();
                        Thread.sleep(25L);
                        return null;
                    });
                    return null;
                }));
            }
            start.countDown();
            await(futures);
        } finally {
            shutdown(executor);
        }

        assertThat(businessCalls.get()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_participant_log", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select try_status from saga_tcc_participant_log", String.class))
                .isEqualTo("SUCCEEDED");
    }

    @Test
    void independentBranchesAreNotSerializedByAProcessWideLock() throws Exception {
        int branches = 12;
        for (int i = 0; i < branches; i++) {
            insertPendingParticipantRow("parallel-saga", i + 1L, "coordinator");
        }
        ExecutorService executor = Executors.newFixedThreadPool(branches);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch insideBusiness = new CountDownLatch(branches);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < branches; i++) {
                final long branchId = i + 1L;
                futures.add(executor.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    execute(command("parallel-saga", branchId, SagaTccAction.TRY, "coordinator"), () -> {
                        int current = running.incrementAndGet();
                        maxRunning.accumulateAndGet(current, Math::max);
                        insideBusiness.countDown();
                        try {
                            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
                        } finally {
                            running.decrementAndGet();
                        }
                        return null;
                    });
                    return null;
                }));
            }
            start.countDown();
            assertThat(insideBusiness.await(10, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            await(futures);
        } finally {
            release.countDown();
            shutdown(executor);
        }

        assertThat(maxRunning.get()).isGreaterThan(1);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_participant_log "
                + "where try_status = 'SUCCEEDED'", Integer.class)).isEqualTo(branches);
    }

    @Test
    void concurrentConfirmAndCancelCanCompleteOnlyOneTerminalAction() throws Exception {
        SagaTccCommandMessage initialTry = command("terminal-race", 3L, SagaTccAction.TRY, "coordinator");
        execute(initialTry, () -> null);

        SagaTccCommandMessage confirm = command("terminal-race", 3L, SagaTccAction.CONFIRM, "coordinator");
        SagaTccCommandMessage cancel = command("terminal-race", 3L, SagaTccAction.CANCEL, "coordinator");
        AtomicInteger confirmCalls = new AtomicInteger();
        AtomicInteger cancelCalls = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> confirmResult = executor.submit(() -> executeCapturing(start, confirm, () -> {
                confirmCalls.incrementAndGet();
                Thread.sleep(25L);
                return null;
            }));
            Future<Throwable> cancelResult = executor.submit(() -> executeCapturing(start, cancel, () -> {
                cancelCalls.incrementAndGet();
                Thread.sleep(25L);
                return null;
            }));
            start.countDown();

            List<Throwable> failures = new ArrayList<Throwable>();
            Throwable confirmFailure = confirmResult.get(20, TimeUnit.SECONDS);
            Throwable cancelFailure = cancelResult.get(20, TimeUnit.SECONDS);
            if (confirmFailure != null) {
                failures.add(confirmFailure);
            }
            if (cancelFailure != null) {
                failures.add(cancelFailure);
            }

            assertThat(confirmCalls.get() + cancelCalls.get()).isEqualTo(1);
            assertThat(failures).hasSize(1);
            assertThat(failures.get(0)).isInstanceOf(SagaTccNonRetryableException.class);
            Integer completed = jdbc.queryForObject("select (case when confirm_status = 'SUCCEEDED' then 1 else 0 end) "
                    + "+ (case when cancel_status = 'SUCCEEDED' then 1 else 0 end) "
                    + "from saga_tcc_participant_log", Integer.class);
            assertThat(completed).isEqualTo(1);
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void sameSagaAndBranchFromDifferentCoordinatorsHaveIndependentIdempotencyKeys() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        execute(command("shared-saga-id", 9L, SagaTccAction.TRY, "coordinator-a"), () -> {
            calls.incrementAndGet();
            return null;
        });
        execute(command("shared-saga-id", 9L, SagaTccAction.TRY, "coordinator-b"), () -> {
            calls.incrementAndGet();
            return null;
        });

        assertThat(calls.get()).isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_participant_log", Integer.class)).isEqualTo(2);
    }

    @Test
    void nonDuplicateConstraintFailureIsNotIgnoredOrAllowedToReachBusinessCode() {
        AtomicInteger businessCalls = new AtomicInteger();
        SagaTccCommandMessage command = command("invalid-insert", 1L, SagaTccAction.TRY, "coordinator");

        assertThatThrownBy(() -> transaction.execute(status -> {
            try {
                repository.executeIdempotently(null, command, () -> {
                    businessCalls.incrementAndGet();
                    return null;
                });
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        })).isInstanceOf(DataIntegrityViolationException.class);

        assertThat(businessCalls.get()).isZero();
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_participant_log", Integer.class)).isZero();
    }

    @Test
    void configuredSchemaRoutesParticipantLogThroughTheBusinessTransaction() {
        jdbc.execute("create schema `saga_store`");
        createParticipantLogTable("`saga_store`.");
        SagaTccProperties properties = new SagaTccProperties();
        properties.setSchema("saga_store");
        properties.afterPropertiesSet();
        JdbcParticipantLogRepository schemaRepository = new JdbcParticipantLogRepository(jdbc, properties);
        SagaTccCommandMessage command = command("schema-saga", 11L, SagaTccAction.TRY, "coordinator");

        transaction.execute(status -> {
            try {
                schemaRepository.executeIdempotently("student-service", command, () -> null);
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        });

        assertThat(jdbc.queryForObject(
                "select count(*) from `saga_store`.saga_tcc_participant_log where try_status = 'SUCCEEDED'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from saga_tcc_participant_log where saga_id = 'schema-saga'", Integer.class))
                .isZero();
    }

    private Throwable executeCapturing(CountDownLatch start, SagaTccCommandMessage command,
                                       Callable<Void> businessCall) throws InterruptedException {
        assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
        try {
            execute(command, businessCall);
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    private void execute(SagaTccCommandMessage command, Callable<Void> businessCall) {
        transaction.execute(status -> {
            try {
                repository.executeIdempotently("student-service", command, businessCall);
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        });
    }

    private SagaTccCommandMessage command(String sagaId, long branchId, SagaTccAction action,
                                          String coordinatorApp) {
        SagaTccCommandMessage command = new SagaTccCommandMessage();
        command.setMessageKey(sagaId + "-" + branchId + "-" + action + "-1");
        command.setSagaId(sagaId);
        command.setBranchId(branchId);
        command.setCoordinatorApp(coordinatorApp);
        command.setTargetApp("student-service");
        command.setBusCode("enroll");
        command.setAction(action);
        command.setAttempt(1);
        command.setRequestClass("example.EnrollmentRequest");
        command.setRequestJson("{\"studentId\":42}");
        return command;
    }

    private void insertPendingParticipantRow(String sagaId, long branchId, String coordinatorApp) throws Exception {
        String json = "{\"studentId\":42}";
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8));
        StringBuilder hash = new StringBuilder(digest.length * 2);
        for (byte current : digest) {
            hash.append(String.format("%02x", current & 0xff));
        }
        jdbc.update("insert into saga_tcc_participant_log (local_app, coordinator_app, saga_id, branch_id, "
                        + "target_app, bus_code, request_hash, create_time, update_time) "
                        + "values ('student-service', ?, ?, ?, 'student-service', 'enroll', ?, "
                        + "current_timestamp, current_timestamp)",
                coordinatorApp, sagaId, branchId, hash.toString());
    }

    private void await(List<Future<?>> futures) throws Exception {
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
    }

    private void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
}
