package com.sagatcc.spring.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 使用真实数据库引擎和真实并发连接验证生产 SQL。
 * H2 以 MySQL 兼容模式运行，因此也会解析 MySQL 特有的抢占语法
 *（UPDATE ... ORDER BY ... LIMIT 和 ON DUPLICATE KEY UPDATE）。
 */
class JdbcSagaTccRepositoryIntegrationTest {

    private static final Timestamp PAST = new Timestamp(1_000L);

    private JdbcTemplate jdbc;
    private JdbcSagaTccRepository repository;
    private SagaTccProperties properties;
    private long nextBranchId;
    private long nextOutboxId;

    @BeforeEach
    void setUp() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:sagatcc_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=30000");
        dataSource.setUser("sa");
        dataSource.setPassword("");

        jdbc = new JdbcTemplate((DataSource) dataSource);
        createSchema();

        properties = new SagaTccProperties();
        properties.setMaxAttempts(3);
        properties.setRetryBaseDelayMillis(5_000L);
        properties.setRetryMaxDelayMillis(60_000L);
        properties.setRetryJitterPercent(0);
        properties.setScanBatchSize(1_000);
        properties.setOutboxClaimBatchSize(16);
        properties.setOutboxClaimTimeoutMillis(30_000L);
        properties.getRocketmq().setSendTimeoutMillis(1_000L);
        properties.afterPropertiesSet();
        repository = new JdbcSagaTccRepository(jdbc, properties);

        nextBranchId = 100L;
        nextOutboxId = 1_000L;
    }

    @Test
    void concurrentBatchClaimsNeverReturnTheSameOutbox() throws Exception {
        insertTransaction("saga-claim", "TRYING");
        final int outboxCount = 96;
        for (int i = 0; i < outboxCount; i++) {
            long branchId = insertBranch("saga-claim", i, "TRYING", 1, 0, 0, PAST);
            insertOutbox("saga-claim", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);
        }

        int workerCount = 12;
        ExecutorService workers = Executors.newFixedThreadPool(workerCount);
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> claimedIds = Collections.synchronizedSet(new HashSet<Long>());
        Set<String> claimTokens = Collections.synchronizedSet(new HashSet<String>());
        AtomicInteger duplicateClaims = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < workerCount; i++) {
                futures.add(workers.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    while (true) {
                        List<SagaTccOutboxRecord> claimed = repository.claimReadyOutbox(5);
                        if (claimed.isEmpty()) {
                            return null;
                        }
                        for (SagaTccOutboxRecord record : claimed) {
                            if (!claimedIds.add(record.getId())) {
                                duplicateClaims.incrementAndGet();
                            }
                            claimTokens.add(record.getClaimToken());
                        }
                    }
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(duplicateClaims.get()).isZero();
        assertThat(claimedIds).hasSize(outboxCount);
        assertThat(claimTokens).doesNotContainNull();
        assertThat(jdbc.queryForObject(
                "select count(*) from saga_tcc_outbox where status = 'SENDING'", Integer.class))
                .isEqualTo(outboxCount);
        assertThat(jdbc.queryForObject(
                "select count(distinct id) from saga_tcc_outbox where claim_token is not null", Integer.class))
                .isEqualTo(outboxCount);
    }

    @Test
    void claimRotationGivesExpiredSendingFailedAndNewRowsAChanceWithoutCrossStatusRangeScan() {
        insertTransaction("saga-status-fairness", "TRYING");
        long branchId = insertBranch("saga-status-fairness", 0, "TRYING", 1, 0, 0, PAST);
        long sending = insertOutbox("saga-status-fairness", branchId, SagaTccAction.TRY, 1,
                "SENDING", 0, PAST);
        long stillWaitingSending = insertOutbox("saga-status-fairness", branchId, SagaTccAction.TRY, 1,
                "SENDING", 0, PAST);
        long failed = insertOutbox("saga-status-fairness", branchId, SagaTccAction.TRY, 1,
                "FAILED", 0, PAST);
        long fresh = insertOutbox("saga-status-fairness", branchId, SagaTccAction.TRY, 1,
                "NEW", 0, PAST);

        assertThat(repository.claimReadyOutbox(1)).extracting(SagaTccOutboxRecord::getId)
                .containsExactly(sending);
        assertThat(repository.claimReadyOutbox(1)).extracting(SagaTccOutboxRecord::getId)
                .containsExactly(failed);
        assertThat(repository.claimReadyOutbox(1)).extracting(SagaTccOutboxRecord::getId)
                .containsExactly(fresh);
        assertThat(jdbc.queryForObject("select next_retry_time from saga_tcc_outbox where id = ?",
                Timestamp.class, stillWaitingSending)).isEqualTo(PAST);
    }

    @Test
    void emptyEarlierStatusesDoNotReduceTheRequestedClaimBatch() {
        insertTransaction("saga-status-fill", "TRYING");
        Set<Long> expected = new HashSet<Long>();
        for (int i = 0; i < 7; i++) {
            long branchId = insertBranch("saga-status-fill", i, "TRYING", 1, 0, 0, PAST);
            expected.add(insertOutbox("saga-status-fill", branchId, SagaTccAction.TRY, 1,
                    "NEW", 0, PAST));
        }

        assertThat(repository.claimReadyOutbox(7))
                .extracting(SagaTccOutboxRecord::getId)
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void duplicateOutboxKeyIsANoOpButOtherConstraintViolationsAreNotIgnored() {
        insertTransaction("saga-duplicate-insert", "TRYING");
        long branchId = insertBranch("saga-duplicate-insert", 0, "TRYING", 1, 0, 0, PAST);

        repository.enqueueOutbox("stable-key", "saga-duplicate-insert", branchId,
                "command-topic", "command", SagaTccAction.TRY, 1, "{\"version\":1}");
        repository.enqueueOutbox("stable-key", "saga-duplicate-insert", branchId,
                "different-topic", "command", SagaTccAction.TRY, 1, "{\"version\":2}");

        assertThat(jdbc.queryForObject(
                "select count(*) from saga_tcc_outbox where message_key = 'stable-key'", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select payload from saga_tcc_outbox where message_key = 'stable-key'", String.class))
                .isEqualTo("{\"version\":1}");
        String oversizedTopic = String.join("", Collections.nCopies(256, "t"));
        assertThatThrownBy(() -> repository.enqueueOutbox("invalid-key", "saga-duplicate-insert", branchId,
                oversizedTopic, "command", SagaTccAction.TRY, 1, "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void pendingCurrentCommandBlocksBusinessRetryUntilDeliveryIsExhausted() {
        insertTransaction("saga-pending", "TRYING");
        long branchId = insertBranch("saga-pending", 0, "TRYING", 1, 0, 0, PAST);
        long outboxId = insertOutbox("saga-pending", branchId, SagaTccAction.TRY, 1,
                "FAILED", properties.getMaxAttempts() - 1, PAST);

        assertThat(repository.findRetryableBranches()).isEmpty();

        jdbc.update("update saga_tcc_outbox set attempts = ? where id = ?",
                properties.getMaxAttempts(), outboxId);

        assertThat(repository.findRetryableBranches())
                .extracting(SagaTccBranchRecord::getId)
                .containsExactly(branchId);
    }

    @Test
    void finalDeliveryFailureBecomesDeadAndImmediatelyUnblocksBusinessRecovery() {
        insertTransaction("saga-dead", "TRYING");
        long branchId = insertBranch("saga-dead", 0, "TRYING", 1, 0, 0, PAST);
        long outboxId = insertOutbox("saga-dead", branchId, SagaTccAction.TRY, 1,
                "FAILED", properties.getMaxAttempts() - 1, PAST);

        SagaTccOutboxRecord lastClaim = repository.claimReadyOutbox(1).get(0);
        repository.markOutboxFailed(lastClaim, properties.getMaxAttempts());

        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, outboxId)).isEqualTo("DEAD");
        assertThat(repository.findRetryableBranches())
                .extracting(SagaTccBranchRecord::getId)
                .containsExactly(branchId);
    }

    @Test
    void actionSwitchDiscardsOldActiveOutboxInsteadOfPollutingReadyIndex() {
        insertTransaction("saga-switch", "CANCELLING");
        long branchId = insertBranch("saga-switch", 0, "TRYING", 1, 0, 0, PAST);
        long tryOutbox = insertOutbox("saga-switch", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);
        long cancelOutbox = insertOutbox("saga-switch", branchId, SagaTccAction.CANCEL, 1, "NEW", 0, PAST);

        repository.markActionDispatched(branchId, SagaTccBranchStatus.CANCELLING, 1);

        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, tryOutbox)).isEqualTo("DISCARDED");
        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, cancelOutbox)).isEqualTo("NEW");
        assertThat(repository.claimReadyOutbox(10))
                .extracting(SagaTccOutboxRecord::getId)
                .containsExactly(cancelOutbox);
    }

    @Test
    void recoveryScanExcludesBranchesWhoseParentPhaseDoesNotMatch() {
        for (int i = 0; i < 120; i++) {
            String sagaId = "stale-" + i;
            insertTransaction(sagaId, "FAILED");
            insertBranch(sagaId, 0, "CONFIRMING", 1, 1, 0, PAST);
        }
        insertTransaction("live-saga", "TRYING");
        long liveBranch = insertBranch("live-saga", 0, "TRYING", 1, 0, 0, PAST);

        assertThat(repository.findRetryableBranches())
                .extracting(SagaTccBranchRecord::getId)
                .containsExactly(liveBranch);
    }

    @Test
    void recoveryStatusRotationPreventsOneBusyPhaseFromStarvingTheOthers() {
        properties.setScanBatchSize(1);
        repository = new JdbcSagaTccRepository(jdbc, properties);
        insertTransaction("retry-try", "TRYING");
        long trying = insertBranch("retry-try", 0, "TRYING", 1, 0, 0, PAST);
        insertTransaction("retry-confirm", "COMMITTING");
        long confirming = insertBranch("retry-confirm", 0, "CONFIRMING", 1, 1, 0, PAST);
        insertTransaction("retry-cancel", "CANCELLING");
        long cancelling = insertBranch("retry-cancel", 0, "CANCELLING", 1, 0, 1, PAST);

        assertThat(repository.findRetryableBranches()).extracting(SagaTccBranchRecord::getId)
                .containsExactly(trying);
        assertThat(repository.findRetryableBranches()).extracting(SagaTccBranchRecord::getId)
                .containsExactly(confirming);
        assertThat(repository.findRetryableBranches()).extracting(SagaTccBranchRecord::getId)
                .containsExactly(cancelling);
    }

    @Test
    void recoveryFillsTheBatchWhenOnlyTheLastRotatedPhaseHasWork() {
        properties.setScanBatchSize(5);
        repository = new JdbcSagaTccRepository(jdbc, properties);
        insertTransaction("retry-cancel-only", "CANCELLING");
        List<Long> expected = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            expected.add(insertBranch("retry-cancel-only", i, "CANCELLING", 1, 0, 1, PAST));
        }

        assertThat(repository.findRetryableBranches()).extracting(SagaTccBranchRecord::getId)
                .containsExactlyElementsOf(expected);
    }

    @Test
    void staleOrDifferentAttemptOutboxDoesNotBlockCurrentBusinessRetry() {
        insertTransaction("saga-stale-attempt", "TRYING");
        long branchId = insertBranch("saga-stale-attempt", 0, "TRYING", 2, 0, 0, PAST);
        insertOutbox("saga-stale-attempt", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);
        insertOutbox("saga-stale-attempt", branchId, SagaTccAction.CONFIRM, 2, "NEW", 0, PAST);

        assertThat(repository.findRetryableBranches())
                .extracting(SagaTccBranchRecord::getId)
                .containsExactly(branchId);
    }

    @Test
    void expiredClaimTokenCannotMarkReclaimedOutboxOrAdvanceDeadline() {
        insertTransaction("saga-lease", "TRYING");
        long branchId = insertBranch("saga-lease", 0, "TRYING", 1, 0, 0, PAST);
        long outboxId = insertOutbox("saga-lease", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);

        SagaTccOutboxRecord expiredOwner = repository.claimReadyOutbox(1).get(0);
        jdbc.update("update saga_tcc_outbox set next_retry_time = ? where id = ?", PAST, outboxId);
        SagaTccOutboxRecord currentOwner = repository.claimReadyOutbox(1).get(0);
        assertThat(currentOwner.getClaimToken()).isNotEqualTo(expiredOwner.getClaimToken());

        long deadlineBeforeStaleWrites = branchDeadline(branchId);
        repository.markOutboxSent(expiredOwner);
        repository.markOutboxFailed(expiredOwner, 1);

        assertThat(branchDeadline(branchId)).isEqualTo(deadlineBeforeStaleWrites);
        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, outboxId)).isEqualTo("SENDING");
        assertThat(jdbc.queryForObject("select claim_token from saga_tcc_outbox where id = ?",
                String.class, outboxId)).isEqualTo(currentOwner.getClaimToken());
        assertThat(jdbc.queryForObject("select attempts from saga_tcc_outbox where id = ?",
                Integer.class, outboxId)).isZero();
    }

    @Test
    void responseDeadlineStartsOnlyAfterTheCurrentClaimIsMarkedSent() {
        insertTransaction("saga-deadline", "TRYING");
        long branchId = insertBranch("saga-deadline", 0, "TRYING", 1, 0, 0, PAST);
        long outboxId = insertOutbox("saga-deadline", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);
        long originalDeadline = branchDeadline(branchId);

        SagaTccOutboxRecord firstClaim = repository.claimReadyOutbox(1).get(0);
        assertThat(branchDeadline(branchId)).isEqualTo(originalDeadline);
        repository.markOutboxFailed(firstClaim, 1);
        assertThat(branchDeadline(branchId)).isEqualTo(originalDeadline);

        jdbc.update("update saga_tcc_outbox set next_retry_time = ? where id = ?", PAST, outboxId);
        SagaTccOutboxRecord secondClaim = repository.claimReadyOutbox(1).get(0);
        assertThat(branchDeadline(branchId)).isEqualTo(originalDeadline);

        long sendStartedAt = System.currentTimeMillis();
        repository.markOutboxSent(secondClaim);

        assertThat(branchDeadline(branchId)).isGreaterThanOrEqualTo(
                sendStartedAt + repository.nextDelayMillis(1) - 100L);
        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, outboxId)).isEqualTo("SENT");
        assertThat(jdbc.queryForObject("select claim_token from saga_tcc_outbox where id = ?",
                String.class, outboxId)).isNull();
    }

    @Test
    void phaseSwitchMakesOldActionUnclaimableAndFinalPhaseClaimsNothing() {
        insertTransaction("saga-phase", "CANCELLING");
        long branchId = insertBranch("saga-phase", 0, "CANCELLING", 1, 0, 1, PAST);
        long staleTryId = insertOutbox("saga-phase", branchId, SagaTccAction.TRY, 1, "NEW", 0, PAST);
        long currentCancelId = insertOutbox("saga-phase", branchId, SagaTccAction.CANCEL, 1, "NEW", 0, PAST);

        assertThat(repository.claimReadyOutbox(10))
                .extracting(SagaTccOutboxRecord::getId)
                .containsExactly(currentCancelId);
        assertThat(jdbc.queryForObject("select status from saga_tcc_outbox where id = ?",
                String.class, staleTryId)).isEqualTo("NEW");

        jdbc.update("update saga_tcc_transaction set status = 'CANCELLED' where saga_id = 'saga-phase'");
        jdbc.update("update saga_tcc_branch set status = 'CANCELLED' where id = ?", branchId);
        jdbc.update("update saga_tcc_outbox set status = 'NEW', claim_token = null, next_retry_time = ? where id = ?",
                PAST, currentCancelId);

        assertThat(repository.claimReadyOutbox(10)).isEmpty();
        assertThat(jdbc.queryForObject(
                "select count(*) from saga_tcc_outbox where status = 'NEW'", Integer.class)).isEqualTo(2);
    }

    @Test
    void configuredSchemaRoutesCoordinatorAndOutboxTablesThroughTheSameDataSource() {
        jdbc.execute("create schema `saga_store`");
        createSchema("`saga_store`.");
        SagaTccProperties schemaProperties = new SagaTccProperties();
        schemaProperties.setSchema("saga_store");
        schemaProperties.setRetryJitterPercent(0);
        schemaProperties.afterPropertiesSet();
        JdbcSagaTccRepository schemaRepository = new JdbcSagaTccRepository(jdbc, schemaProperties);

        SagaTccContext context = new SagaTccContext("schema-saga", "coordinator", "enroll", "business-1");
        schemaRepository.insertTransaction(context, 1);
        long branchId = schemaRepository.insertBranch("schema-saga", 1, "student-service", "enroll",
                "example.EnrollRequest", "{}");
        schemaRepository.enqueueOutbox("schema-message", "schema-saga", branchId,
                "command-topic", "command-tag", SagaTccAction.TRY, 1, "{}");
        schemaRepository.markActionDispatched(branchId, SagaTccBranchStatus.TRYING, 1);
        schemaRepository.updateTransactionStatus("schema-saga", SagaTccTransactionStatus.TRYING);

        List<SagaTccOutboxRecord> claimed = schemaRepository.claimReadyOutbox(1);
        assertThat(claimed).hasSize(1);
        schemaRepository.markOutboxSent(claimed.get(0));

        assertThat(jdbc.queryForObject(
                "select count(*) from `saga_store`.saga_tcc_transaction", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from `saga_store`.saga_tcc_branch", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from `saga_store`.saga_tcc_outbox where status = 'SENT'", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from saga_tcc_transaction where saga_id = 'schema-saga'", Integer.class))
                .isZero();
    }

    private void createSchema() {
        createSchema("");
    }

    private void createSchema(String prefix) {
        jdbc.execute("create table " + prefix + "saga_tcc_transaction ("
                + "saga_id varchar(64) not null primary key, "
                + "coordinator_app varchar(128) not null, business_code varchar(128) not null, "
                + "business_id varchar(128) not null, status varchar(32) not null, branch_count int not null, "
                + "last_error varchar(2000), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null)");
        jdbc.execute("create table " + prefix + "saga_tcc_branch ("
                + "id bigint auto_increment primary key, saga_id varchar(64) not null, branch_no int not null, "
                + "target_app varchar(128) not null, bus_code varchar(128) not null, "
                + "request_class varchar(512) not null, request_json longtext not null, status varchar(32) not null, "
                + "try_attempts int not null default 0, confirm_attempts int not null default 0, "
                + "cancel_attempts int not null default 0, failure_attempt int not null default 0, "
                + "last_error varchar(2000), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null, "
                + "unique (saga_id, branch_no), "
                + "index idx_saga_tcc_branch_status_retry (status, next_retry_time, id))");
        jdbc.execute("create table " + prefix + "saga_tcc_outbox ("
                + "id bigint auto_increment primary key, message_key varchar(160) not null unique, "
                + "saga_id varchar(64) not null, branch_id bigint not null, topic varchar(255) not null, "
                + "tag varchar(64) not null, action varchar(32) not null, command_attempt int not null, "
                + "payload longtext not null, status varchar(32) not null, attempts int not null default 0, "
                + "claim_token varchar(64), next_retry_time timestamp(3) not null, "
                + "create_time timestamp(3) not null, update_time timestamp(3) not null)");
    }

    private void insertTransaction(String sagaId, String status) {
        jdbc.update("insert into saga_tcc_transaction "
                        + "(saga_id, coordinator_app, business_code, business_id, status, branch_count, "
                        + "next_retry_time, create_time, update_time) values (?, 'coordinator', 'enroll', ?, ?, 0, "
                        + "?, current_timestamp, current_timestamp)",
                sagaId, sagaId, status, PAST);
    }

    private long insertBranch(String sagaId, int branchNo, String status,
                              int tryAttempts, int confirmAttempts, int cancelAttempts,
                              Timestamp nextRetryTime) {
        long branchId = ++nextBranchId;
        jdbc.update("insert into saga_tcc_branch "
                        + "(id, saga_id, branch_no, target_app, bus_code, request_class, request_json, status, "
                        + "try_attempts, confirm_attempts, cancel_attempts, failure_attempt, next_retry_time, "
                        + "create_time, update_time) values (?, ?, ?, 'student-service', 'enroll', "
                        + "'example.EnrollRequest', '{}', ?, ?, ?, ?, 0, ?, current_timestamp, current_timestamp)",
                branchId, sagaId, branchNo, status, tryAttempts, confirmAttempts, cancelAttempts, nextRetryTime);
        return branchId;
    }

    private long insertOutbox(String sagaId, long branchId, SagaTccAction action, int commandAttempt,
                              String status, int attempts, Timestamp nextRetryTime) {
        long outboxId = ++nextOutboxId;
        jdbc.update("insert into saga_tcc_outbox "
                        + "(id, message_key, saga_id, branch_id, topic, tag, action, command_attempt, payload, "
                        + "status, attempts, next_retry_time, create_time, update_time) "
                        + "values (?, ?, ?, ?, 'command-topic', 'command-tag', ?, ?, '{}', ?, ?, ?, "
                        + "current_timestamp, current_timestamp)",
                outboxId, "message-" + outboxId, sagaId, branchId, action.name(), commandAttempt,
                status, attempts, nextRetryTime);
        return outboxId;
    }

    private long branchDeadline(long branchId) {
        return jdbc.queryForObject("select next_retry_time from saga_tcc_branch where id = ?",
                Timestamp.class, branchId).getTime();
    }
}
