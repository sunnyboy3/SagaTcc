package com.sagatcc.spring.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.store.JdbcSagaTccRepository;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

class SagaTccCoordinatorJdbcConcurrencyIntegrationTest {

    private static final int BRANCHES = 24;

    private JdbcTemplate jdbc;
    private SagaTccCoordinator coordinator;
    private TransactionTemplate transaction;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:coordinator_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=30000");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        jdbc = new JdbcTemplate(dataSource);
        createSchema();

        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("coordinator");
        properties.setRetryJitterPercent(0);
        JdbcSagaTccRepository repository = new JdbcSagaTccRepository(jdbc, properties);
        coordinator = new SagaTccCoordinator(repository, new NoOpPublisher(), new ObjectMapper(),
                properties, "coordinator");
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    @Test
    void concurrentLastTryResultsAlwaysAdvanceEveryBranchToConfirm() throws Exception {
        insertSaga("try-race", SagaTccTransactionStatus.TRYING, SagaTccBranchStatus.TRYING,
                1, 0, 0);

        deliverConcurrently("try-race", SagaTccAction.TRY);

        assertThat(transactionStatus("try-race")).isEqualTo(SagaTccTransactionStatus.COMMITTING.name());
        assertThat(branchesIn("try-race", SagaTccBranchStatus.CONFIRMING)).isEqualTo(BRANCHES);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_outbox where saga_id = ? "
                + "and action = 'CONFIRM' and command_attempt = 1", Integer.class, "try-race"))
                .isEqualTo(BRANCHES);
        assertThat(jdbc.queryForObject("select count(distinct message_key) from saga_tcc_outbox where saga_id = ?",
                Integer.class, "try-race")).isEqualTo(BRANCHES);
    }

    @Test
    void concurrentLastConfirmResultsCannotLeaveCommittedSagaBehind() throws Exception {
        insertSaga("confirm-race", SagaTccTransactionStatus.COMMITTING, SagaTccBranchStatus.CONFIRMING,
                1, 1, 0);

        deliverConcurrently("confirm-race", SagaTccAction.CONFIRM);

        assertThat(transactionStatus("confirm-race")).isEqualTo(SagaTccTransactionStatus.COMMITTED.name());
        assertThat(branchesIn("confirm-race", SagaTccBranchStatus.CONFIRMED)).isEqualTo(BRANCHES);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_outbox", Integer.class)).isZero();
    }

    @Test
    void concurrentLastCancelResultsCannotLeaveCancelledSagaBehind() throws Exception {
        insertSaga("cancel-race", SagaTccTransactionStatus.CANCELLING, SagaTccBranchStatus.CANCELLING,
                1, 0, 1);

        deliverConcurrently("cancel-race", SagaTccAction.CANCEL);

        assertThat(transactionStatus("cancel-race")).isEqualTo(SagaTccTransactionStatus.CANCELLED.name());
        assertThat(branchesIn("cancel-race", SagaTccBranchStatus.CANCELLED)).isEqualTo(BRANCHES);
        assertThat(jdbc.queryForObject("select count(*) from saga_tcc_outbox", Integer.class)).isZero();
    }

    private void deliverConcurrently(String sagaId, SagaTccAction action) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(BRANCHES);
        CountDownLatch ready = new CountDownLatch(BRANCHES);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<Future<?>>();
        try {
            for (long branchId = 1; branchId <= BRANCHES; branchId++) {
                final long currentBranchId = branchId;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    transaction.execute(status -> {
                        coordinator.handleResult(result(sagaId, currentBranchId, action));
                        return null;
                    });
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private SagaTccResultMessage result(String sagaId, long branchId, SagaTccAction action) {
        SagaTccResultMessage result = new SagaTccResultMessage();
        result.setMessageKey(sagaId + "-" + branchId + "-" + action + "-1-result");
        result.setSagaId(sagaId);
        result.setBranchId(branchId);
        result.setCoordinatorApp("coordinator");
        result.setTargetApp("student-service");
        result.setBusCode("enroll");
        result.setAction(action);
        result.setAttempt(1);
        result.setSuccess(true);
        result.setRetryable(false);
        return result;
    }

    private void insertSaga(String sagaId, SagaTccTransactionStatus transactionStatus,
                            SagaTccBranchStatus branchStatus, int tryAttempts,
                            int confirmAttempts, int cancelAttempts) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbc.update("insert into saga_tcc_transaction (saga_id, coordinator_app, business_code, business_id, "
                        + "status, branch_count, next_retry_time, create_time, update_time) "
                        + "values (?, 'coordinator', 'enroll', ?, ?, ?, ?, ?, ?)",
                sagaId, sagaId, transactionStatus.name(), BRANCHES, now, now, now);
        for (int branch = 1; branch <= BRANCHES; branch++) {
            jdbc.update("insert into saga_tcc_branch (id, saga_id, branch_no, target_app, bus_code, "
                            + "request_class, request_json, status, try_attempts, confirm_attempts, cancel_attempts, "
                            + "failure_attempt, next_retry_time, create_time, update_time) "
                            + "values (?, ?, ?, 'student-service', 'enroll', 'example.EnrollmentRequest', '{}', "
                            + "?, ?, ?, ?, 0, ?, ?, ?)",
                    branch, sagaId, branch, branchStatus.name(), tryAttempts, confirmAttempts, cancelAttempts,
                    now, now, now);
        }
    }

    private int branchesIn(String sagaId, SagaTccBranchStatus status) {
        return jdbc.queryForObject("select count(*) from saga_tcc_branch where saga_id = ? and status = ?",
                Integer.class, sagaId, status.name());
    }

    private String transactionStatus(String sagaId) {
        return jdbc.queryForObject("select status from saga_tcc_transaction where saga_id = ?",
                String.class, sagaId);
    }

    private void createSchema() {
        jdbc.execute("create table saga_tcc_transaction (saga_id varchar(64) primary key, "
                + "coordinator_app varchar(128) not null, business_code varchar(128) not null, "
                + "business_id varchar(128) not null, status varchar(32) not null, branch_count int not null, "
                + "last_error varchar(2000), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null)");
        jdbc.execute("create table saga_tcc_branch (id bigint primary key, saga_id varchar(64) not null, "
                + "branch_no int not null, target_app varchar(128) not null, bus_code varchar(128) not null, "
                + "request_class varchar(512) not null, request_json longtext not null, status varchar(32) not null, "
                + "try_attempts int not null, confirm_attempts int not null, cancel_attempts int not null, "
                + "failure_attempt int not null, last_error varchar(2000), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null, unique(saga_id, branch_no))");
        jdbc.execute("create table saga_tcc_outbox (id bigint auto_increment primary key, "
                + "message_key varchar(160) not null unique, saga_id varchar(64) not null, branch_id bigint not null, "
                + "topic varchar(255) not null, tag varchar(64) not null, action varchar(32) not null, "
                + "command_attempt int not null, payload longtext not null, status varchar(32) not null, "
                + "attempts int not null, claim_token varchar(64), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null)");
    }

    private static final class NoOpPublisher implements SagaMessagePublisher {

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
        }
    }
}
