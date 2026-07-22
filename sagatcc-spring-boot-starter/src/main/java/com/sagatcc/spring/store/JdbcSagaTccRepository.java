package com.sagatcc.spring.store;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccOutboxStatus;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccProperties;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

public class JdbcSagaTccRepository implements SagaTccRepository, SagaTccDataSourceProvider {

    private static final SagaTccOutboxStatus[] CLAIMABLE_OUTBOX_STATUSES = {
            SagaTccOutboxStatus.SENDING, SagaTccOutboxStatus.FAILED, SagaTccOutboxStatus.NEW
    };
    private static final SagaTccBranchStatus[] RETRYABLE_BRANCH_STATUSES = {
            SagaTccBranchStatus.TRYING, SagaTccBranchStatus.CONFIRMING, SagaTccBranchStatus.CANCELLING
    };
    private static final int OUTBOX_CLAIM_MAX_ATTEMPTS = 3;
    private static final long OUTBOX_CLAIM_RETRY_BASE_DELAY_MILLIS = 10L;

    private final JdbcTemplate jdbcTemplate;
    private final SagaTccProperties properties;
    private final String transactionTable;
    private final String branchTable;
    private final String outboxTable;
    private final AtomicInteger outboxStatusCursor = new AtomicInteger();
    private final AtomicInteger branchStatusCursor = new AtomicInteger();

    private final RowMapper<SagaTccBranchRecord> branchMapper = (rs, rowNum) -> {
        SagaTccBranchRecord record = new SagaTccBranchRecord();
        record.setId(rs.getLong("id"));
        record.setSagaId(rs.getString("saga_id"));
        record.setBranchNo(rs.getInt("branch_no"));
        record.setTargetApp(rs.getString("target_app"));
        record.setBusCode(rs.getString("bus_code"));
        record.setRequestClass(rs.getString("request_class"));
        record.setRequestJson(rs.getString("request_json"));
        record.setStatus(SagaTccBranchStatus.valueOf(rs.getString("status")));
        record.setTryAttempts(rs.getInt("try_attempts"));
        record.setConfirmAttempts(rs.getInt("confirm_attempts"));
        record.setCancelAttempts(rs.getInt("cancel_attempts"));
        record.setFailureAttempt(rs.getInt("failure_attempt"));
        record.setLastError(rs.getString("last_error"));
        record.setNextRetryTime(rs.getTimestamp("next_retry_time"));
        record.setCreateTime(rs.getTimestamp("create_time"));
        record.setUpdateTime(rs.getTimestamp("update_time"));
        return record;
    };

    private final RowMapper<SagaTccBranchRecord> branchStateMapper = (rs, rowNum) -> {
        SagaTccBranchRecord record = new SagaTccBranchRecord();
        record.setId(rs.getLong("id"));
        record.setSagaId(rs.getString("saga_id"));
        record.setBranchNo(rs.getInt("branch_no"));
        record.setTargetApp(rs.getString("target_app"));
        record.setBusCode(rs.getString("bus_code"));
        record.setStatus(SagaTccBranchStatus.valueOf(rs.getString("status")));
        record.setTryAttempts(rs.getInt("try_attempts"));
        record.setConfirmAttempts(rs.getInt("confirm_attempts"));
        record.setCancelAttempts(rs.getInt("cancel_attempts"));
        record.setFailureAttempt(rs.getInt("failure_attempt"));
        record.setLastError(rs.getString("last_error"));
        record.setNextRetryTime(rs.getTimestamp("next_retry_time"));
        record.setCreateTime(rs.getTimestamp("create_time"));
        record.setUpdateTime(rs.getTimestamp("update_time"));
        return record;
    };

    private final RowMapper<SagaTccTransactionRecord> transactionMapper = (rs, rowNum) -> {
        SagaTccTransactionRecord record = new SagaTccTransactionRecord();
        record.setSagaId(rs.getString("saga_id"));
        record.setCoordinatorApp(rs.getString("coordinator_app"));
        record.setBusinessCode(rs.getString("business_code"));
        record.setBusinessId(rs.getString("business_id"));
        record.setStatus(SagaTccTransactionStatus.valueOf(rs.getString("status")));
        record.setBranchCount(rs.getInt("branch_count"));
        record.setLastError(rs.getString("last_error"));
        record.setNextRetryTime(rs.getTimestamp("next_retry_time"));
        record.setCreateTime(rs.getTimestamp("create_time"));
        record.setUpdateTime(rs.getTimestamp("update_time"));
        return record;
    };

    private final RowMapper<SagaTccOutboxRecord> outboxMapper = (rs, rowNum) -> {
        SagaTccOutboxRecord record = new SagaTccOutboxRecord();
        record.setId(rs.getLong("id"));
        record.setMessageKey(rs.getString("message_key"));
        record.setSagaId(rs.getString("saga_id"));
        record.setBranchId(rs.getLong("branch_id"));
        record.setTopic(rs.getString("topic"));
        record.setTag(rs.getString("tag"));
        record.setPayload(rs.getString("payload"));
        record.setStatus(SagaTccOutboxStatus.valueOf(rs.getString("status")));
        record.setAttempts(rs.getInt("attempts"));
        String action = rs.getString("action");
        record.setAction(action == null ? null : SagaTccAction.valueOf(action));
        record.setCommandAttempt(rs.getInt("command_attempt"));
        record.setClaimToken(rs.getString("claim_token"));
        record.setNextRetryTime(rs.getTimestamp("next_retry_time"));
        record.setCreateTime(rs.getTimestamp("create_time"));
        record.setUpdateTime(rs.getTimestamp("update_time"));
        return record;
    };

    public JdbcSagaTccRepository(JdbcTemplate jdbcTemplate, SagaTccProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        SagaTccTableNames tableNames = new SagaTccTableNames(properties.getSchema());
        this.transactionTable = tableNames.transaction();
        this.branchTable = tableNames.branch();
        this.outboxTable = tableNames.outbox();
    }

    @Override
    public javax.sql.DataSource sagaTccDataSource() {
        return jdbcTemplate.getDataSource();
    }

    @Override
    public void insertTransaction(SagaTccContext context, int branchCount) {
        jdbcTemplate.update("insert into " + transactionTable + " " +
                        "(saga_id, coordinator_app, business_code, business_id, status, branch_count, next_retry_time, create_time, update_time) " +
                        "values (?, ?, ?, ?, ?, ?, current_timestamp(3), current_timestamp(3), current_timestamp(3))",
                context.getSagaId(), context.getCoordinatorApp(), context.getBusinessCode(), context.getBusinessId(),
                SagaTccTransactionStatus.NEW.name(), branchCount);
    }

    @Override
    public long insertBranch(String sagaId, int branchNo, String targetApp, String busCode,
                             String requestClass, String requestJson) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update((PreparedStatementCreator) connection -> {
            PreparedStatement ps = connection.prepareStatement("insert into " + branchTable + " " +
                            "(saga_id, branch_no, target_app, bus_code, request_class, request_json, status, next_retry_time, create_time, update_time) " +
                            "values (?, ?, ?, ?, ?, ?, ?, current_timestamp(3), current_timestamp(3), current_timestamp(3))",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, sagaId);
            ps.setInt(2, branchNo);
            ps.setString(3, targetApp);
            ps.setString(4, busCode);
            ps.setString(5, requestClass);
            ps.setString(6, requestJson);
            ps.setString(7, SagaTccBranchStatus.NEW.name());
            return ps;
        }, keyHolder);
        if (keyHolder.getKey() == null) {
            throw new SagaTccException("database did not return a generated SagaTcc branch id");
        }
        return keyHolder.getKey().longValue();
    }

    @Override
    public void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag, String payload) {
        throw new IllegalArgumentException("outbox action and command attempt are required");
    }

    @Override
    public void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag,
                              SagaTccAction action, int commandAttempt, String payload) {
        if (action == null || commandAttempt <= 0) {
            throw new IllegalArgumentException("outbox action and positive command attempt are required");
        }
        jdbcTemplate.update("insert into " + outboxTable + " " +
                        "(message_key, saga_id, branch_id, topic, tag, action, command_attempt, payload, status, attempts, "
                        + "next_retry_time, create_time, update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, "
                        + "current_timestamp(3), current_timestamp(3), current_timestamp(3)) "
                        + "on duplicate key update id = id",
                messageKey, sagaId, branchId, topic, tag, action == null ? null : action.name(), commandAttempt,
                payload, SagaTccOutboxStatus.NEW.name());
    }

    @Override
    public SagaTccTransactionRecord findTransaction(String sagaId) {
        try {
            return jdbcTemplate.queryForObject("select * from " + transactionTable + " where saga_id = ?",
                    transactionMapper, sagaId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public SagaTccTransactionRecord findTransactionForUpdate(String sagaId) {
        try {
            return jdbcTemplate.queryForObject(
                    "select * from " + transactionTable + " where saga_id = ? for update",
                    transactionMapper, sagaId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public SagaTccBranchRecord findBranch(long branchId) {
        try {
            return jdbcTemplate.queryForObject("select id, saga_id, branch_no, target_app, bus_code, status, "
                            + "try_attempts, confirm_attempts, cancel_attempts, failure_attempt, last_error, next_retry_time, "
                            + "create_time, update_time from " + branchTable + " where id = ?",
                    branchStateMapper, branchId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public SagaTccBranchRecord findBranchWithPayload(long branchId) {
        try {
            return jdbcTemplate.queryForObject("select * from " + branchTable + " where id = ?",
                    branchMapper, branchId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<SagaTccBranchRecord> findBranches(String sagaId) {
        return jdbcTemplate.query("select * from " + branchTable + " where saga_id = ? order by branch_no",
                branchMapper, sagaId);
    }

    @Override
    public boolean allBranchesInStatus(String sagaId, SagaTccBranchStatus status) {
        Integer result = jdbcTemplate.queryForObject("select case when count(*) > 0 "
                        + "and sum(case when status = ? then 1 else 0 end) = count(*) then 1 else 0 end "
                        + "from " + branchTable + " where saga_id = ?",
                Integer.class, status.name(), sagaId);
        return Integer.valueOf(1).equals(result);
    }

    @Override
    public void updateTransactionStatus(String sagaId, SagaTccTransactionStatus status) {
        jdbcTemplate.update("update " + transactionTable
                        + " set status = ?, update_time = current_timestamp(3) where saga_id = ?",
                status.name(), sagaId);
    }

    @Override
    public boolean transitionTransactionStatus(String sagaId, SagaTccTransactionStatus status,
                                               SagaTccTransactionStatus... expectedStatuses) {
        if (expectedStatuses == null || expectedStatuses.length == 0) {
            throw new IllegalArgumentException("expectedStatuses must not be empty");
        }
        StringBuilder sql = new StringBuilder("update " + transactionTable
                + " set status = ?, update_time = current_timestamp(3) "
                + "where saga_id = ? and status in (");
        List<Object> args = new ArrayList<Object>();
        args.add(status.name());
        args.add(sagaId);
        appendStatusPlaceholders(sql, args, expectedStatuses);
        sql.append(')');
        return jdbcTemplate.update(sql.toString(), args.toArray()) == 1;
    }

    @Override
    public boolean completeTransactionPhase(String sagaId,
                                            SagaTccTransactionStatus expectedTransactionStatus,
                                            SagaTccTransactionStatus completedTransactionStatus,
                                            SagaTccBranchStatus completedBranchStatus) {
        return jdbcTemplate.update("update " + transactionTable + " tx set status = case "
                        + "when exists (select 1 from " + branchTable + " failed where failed.saga_id = tx.saga_id "
                        + "and failed.status = 'FAILED') then 'FAILED' else ? end, update_time = current_timestamp(3) "
                        + "where tx.saga_id = ? and tx.status = ? "
                        + "and exists (select 1 from " + branchTable + " present where present.saga_id = tx.saga_id) "
                        + "and not exists (select 1 from " + branchTable + " pending where pending.saga_id = tx.saga_id "
                        + "and pending.status not in (?, 'FAILED'))",
                completedTransactionStatus.name(), sagaId, expectedTransactionStatus.name(),
                completedBranchStatus.name()) == 1;
    }

    @Override
    public void updateBranchStatus(long branchId, SagaTccBranchStatus status, String error) {
        int updated = jdbcTemplate.update(
                "update " + branchTable
                        + " set status = ?, last_error = ?, update_time = current_timestamp(3) where id = ?",
                status.name(), trimError(error), branchId);
        if (updated == 1 && !isDispatchable(status)) {
            discardActiveOutbox(branchId);
        }
    }

    @Override
    public boolean transitionBranchStatus(long branchId, SagaTccBranchStatus expectedStatus,
                                          SagaTccBranchStatus status, String error) {
        boolean updated = jdbcTemplate.update("update " + branchTable
                        + " set status = ?, last_error = ?, update_time = current_timestamp(3) "
                        + "where id = ? and status = ?",
                status.name(), trimError(error), branchId, expectedStatus.name()) == 1;
        if (updated && !isDispatchable(status)) {
            discardActiveOutbox(branchId);
        }
        return updated;
    }

    @Override
    public boolean recordBranchFailure(long branchId, SagaTccBranchStatus expectedStatus,
                                       int attempt, String error) {
        return jdbcTemplate.update("update " + branchTable + " set last_error = ?, failure_attempt = ?, "
                        + "update_time = current_timestamp(3) where id = ? and status = ? and failure_attempt < ?",
                trimError(error), attempt, branchId, expectedStatus.name(), attempt) == 1;
    }

    @Override
    public void markActionDispatched(long branchId, SagaTccBranchStatus status, int attempt) {
        String column = attemptsColumn(status);
        int updated = jdbcTemplate.update("update " + branchTable + " set status = ?, " + column
                        + " = greatest(" + column
                        + ", ?), failure_attempt = 0, next_retry_time = "
                        + "timestampadd(microsecond, ?, current_timestamp(3)), update_time = current_timestamp(3) "
                        + "where id = ?",
                status.name(), attempt, delayMicros(retryDelayMillis(attempt)), branchId);
        if (updated == 1) {
            discardObsoleteOutbox(branchId, actionFor(status), attempt);
        }
    }

    @Override
    public boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                       int currentAttempt, int nextAttempt) {
        return markRetryDispatched(branchId, status, currentAttempt, nextAttempt,
                transactionStatusFor(status));
    }

    @Override
    public boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                       int currentAttempt, int nextAttempt,
                                       SagaTccTransactionStatus expectedTransactionStatus) {
        String column = attemptsColumn(status);
        boolean updated = jdbcTemplate.update("update " + branchTable + " b set " + column
                        + " = ?, next_retry_time = "
                        + "timestampadd(microsecond, ?, current_timestamp(3)), update_time = current_timestamp(3) "
                        + "where id = ? and status = ? and " + column
                        + " = ? and next_retry_time <= current_timestamp(3) and exists (select 1 from "
                        + transactionTable + " tx where tx.saga_id = b.saga_id and tx.status = ?)",
                nextAttempt, delayMicros(retryDelayMillis(nextAttempt)),
                branchId, status.name(), currentAttempt, expectedTransactionStatus.name()) == 1;
        if (updated) {
            discardObsoleteOutbox(branchId, actionFor(status), nextAttempt);
        }
        return updated;
    }

    /** 保留给运维查询使用；发布方发送前必须先抢占记录。 */
    public List<SagaTccOutboxRecord> findReadyOutbox() {
        return jdbcTemplate.query("select * from " + outboxTable + " where status in ('NEW','FAILED','SENDING') " +
                        "and attempts < ? and next_retry_time <= current_timestamp(3) order by id limit ?",
                outboxMapper, properties.getMaxAttempts(), properties.getScanBatchSize());
    }

    @Override
    public SagaTccOutboxRecord claimNextReadyOutbox() {
        List<SagaTccOutboxRecord> records = claimReadyOutbox(1);
        return records.isEmpty() ? null : records.get(0);
    }

    @Override
    public List<SagaTccOutboxRecord> claimReadyOutbox(int limit) {
        if (limit <= 0) {
            return new ArrayList<SagaTccOutboxRecord>();
        }
        int claimLimit = Math.min(limit, properties.getOutboxClaimBatchSize());
        String claimToken = UUID.randomUUID().toString().replace("-", "");
        long sendWindow = safeMultiply(properties.getRocketmq().getSendTimeoutMillis(), claimLimit);
        long leaseMillis = Math.max(properties.getOutboxClaimTimeoutMillis(), safeAdd(sendWindow, 1000L));
        int updated = 0;
        int remaining = claimLimit;
        int start = Math.floorMod(outboxStatusCursor.getAndIncrement(), CLAIMABLE_OUTBOX_STATUSES.length);
        for (int offset = 0; offset < CLAIMABLE_OUTBOX_STATUSES.length && remaining > 0; offset++) {
            SagaTccOutboxStatus sourceStatus = CLAIMABLE_OUTBOX_STATUSES[
                    (start + offset) % CLAIMABLE_OUTBOX_STATUSES.length];
            int claimed = claimReadyOutbox(sourceStatus, claimToken, leaseMillis, remaining);
            updated += claimed;
            remaining -= claimed;
        }
        if (updated == 0) {
            return new ArrayList<SagaTccOutboxRecord>();
        }
        return jdbcTemplate.query("select * from " + outboxTable + " where claim_token = ? order by id",
                outboxMapper, claimToken);
    }

    private int claimReadyOutbox(SagaTccOutboxStatus sourceStatus, String claimToken,
                                 long leaseMillis, int claimLimit) {
        for (int attempt = 1; attempt <= OUTBOX_CLAIM_MAX_ATTEMPTS; attempt++) {
            try {
                return claimReadyOutboxOnce(sourceStatus, claimToken, leaseMillis, claimLimit);
            } catch (PessimisticLockingFailureException e) {
                if (attempt == OUTBOX_CLAIM_MAX_ATTEMPTS) {
                    throw e;
                }
                pauseBeforeOutboxClaimRetry(attempt);
            }
        }
        return 0;
    }

    /**
     * 候选查询只做一致性读取，真正抢占仅按 Outbox 主键加锁。
     * 这样可以避免抢占线程按 Outbox→事务表的顺序加锁，而结果处理线程按事务表→Outbox 的顺序加锁。
     */
    private int claimReadyOutboxOnce(SagaTccOutboxStatus sourceStatus, String claimToken,
                                     long leaseMillis, int claimLimit) {
        List<Long> candidateIds = jdbcTemplate.queryForList(
                "select o.id from " + outboxTable + " o join " + branchTable + " b on b.id = o.branch_id "
                        + "join " + transactionTable + " tx on tx.saga_id = b.saga_id "
                        + "where o.status = ? and o.attempts < ? "
                        + "and o.next_retry_time <= current_timestamp(3) "
                        + "and ((o.action = 'TRY' and b.status = 'TRYING' "
                        + "and b.try_attempts = o.command_attempt and tx.status = 'TRYING') "
                        + "or (o.action = 'CONFIRM' and b.status = 'CONFIRMING' "
                        + "and b.confirm_attempts = o.command_attempt and tx.status = 'COMMITTING') "
                        + "or (o.action = 'CANCEL' and b.status = 'CANCELLING' "
                        + "and b.cancel_attempts = o.command_attempt and tx.status = 'CANCELLING')) "
                        + "order by o.next_retry_time, o.id limit ?",
                Long.class, sourceStatus.name(), properties.getMaxAttempts(), claimLimit);
        if (candidateIds.isEmpty()) {
            return 0;
        }

        StringBuilder sql = new StringBuilder("update ").append(outboxTable)
                .append(" set status = ?, claim_token = ?, next_retry_time = ")
                .append("timestampadd(microsecond, ?, current_timestamp(3)), ")
                .append("update_time = current_timestamp(3) where status = ? and attempts < ? ")
                .append("and next_retry_time <= current_timestamp(3) and id in (");
        List<Object> args = new ArrayList<Object>();
        args.add(SagaTccOutboxStatus.SENDING.name());
        args.add(claimToken);
        args.add(delayMicros(leaseMillis));
        args.add(sourceStatus.name());
        args.add(properties.getMaxAttempts());
        for (int i = 0; i < candidateIds.size(); i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('?');
            args.add(candidateIds.get(i));
        }
        sql.append(") order by id");
        return jdbcTemplate.update(sql.toString(), args.toArray());
    }

    private void pauseBeforeOutboxClaimRetry(int failedAttempt) {
        long minimumDelay = OUTBOX_CLAIM_RETRY_BASE_DELAY_MILLIS * failedAttempt;
        long delay = ThreadLocalRandom.current().nextLong(minimumDelay, minimumDelay * 2L + 1L);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SagaTccException("Outbox 抢占重试等待被中断", e);
        }
    }

    @Override
    public void markOutboxSent(SagaTccOutboxRecord outbox) {
        SagaTccBranchStatus branchStatus = branchStatusFor(outbox.getAction());
        String attemptsColumn = attemptsColumn(branchStatus);
        jdbcTemplate.update("update " + branchTable + " set next_retry_time = "
                        + "timestampadd(microsecond, ?, current_timestamp(3)), update_time = current_timestamp(3) "
                        + "where id = ? "
                        + "and status = ? and " + attemptsColumn + " = ? and exists (select 1 from "
                        + outboxTable + " o "
                        + "where o.id = ? and o.status = 'SENDING' and o.claim_token = ?)",
                delayMicros(retryDelayMillis(outbox.getCommandAttempt())),
                outbox.getBranchId(), branchStatus.name(), outbox.getCommandAttempt(),
                outbox.getId(), outbox.getClaimToken());
        jdbcTemplate.update("update " + outboxTable
                        + " set status = ?, claim_token = null, update_time = current_timestamp(3) "
                        + "where id = ? and status = ? and claim_token = ?",
                SagaTccOutboxStatus.SENT.name(), outbox.getId(), SagaTccOutboxStatus.SENDING.name(),
                outbox.getClaimToken());
    }

    @Override
    public void markOutboxFailed(SagaTccOutboxRecord outbox, int attempts) {
        long delay = retryDelayMillis(attempts);
        SagaTccOutboxStatus status = attempts >= properties.getMaxAttempts()
                ? SagaTccOutboxStatus.DEAD : SagaTccOutboxStatus.FAILED;
        jdbcTemplate.update("update " + outboxTable
                        + " set status = ?, attempts = attempts + 1, claim_token = null, "
                        + "next_retry_time = timestampadd(microsecond, ?, current_timestamp(3)), "
                        + "update_time = current_timestamp(3) where id = ? and status = ? and claim_token = ?",
                status.name(), delayMicros(delay),
                outbox.getId(), SagaTccOutboxStatus.SENDING.name(), outbox.getClaimToken());
    }

    @Override
    public List<SagaTccBranchRecord> findRetryableBranches() {
        List<SagaTccBranchRecord> result = new ArrayList<SagaTccBranchRecord>();
        int remaining = properties.getScanBatchSize();
        int start = Math.floorMod(branchStatusCursor.getAndIncrement(), RETRYABLE_BRANCH_STATUSES.length);
        for (int offset = 0; offset < RETRYABLE_BRANCH_STATUSES.length && remaining > 0; offset++) {
            SagaTccBranchStatus status = RETRYABLE_BRANCH_STATUSES[
                    (start + offset) % RETRYABLE_BRANCH_STATUSES.length];
            List<SagaTccBranchRecord> current = findRetryableBranches(status, remaining);
            result.addAll(current);
            remaining -= current.size();
        }
        return result;
    }

    private List<SagaTccBranchRecord> findRetryableBranches(SagaTccBranchStatus status, int limit) {
        SagaTccTransactionStatus transactionStatus = transactionStatusFor(status);
        SagaTccAction action = actionFor(status);
        String attemptColumn = attemptsColumn(status);
        return jdbcTemplate.query("select b.id, b.saga_id, b.branch_no, b.target_app, b.bus_code, b.status, "
                        + "b.try_attempts, b.confirm_attempts, b.cancel_attempts, b.failure_attempt, "
                        + "b.last_error, b.next_retry_time, b.create_time, b.update_time "
                        + "from " + branchTable + " b where b.status = ? "
                        + "and (select tx.status from " + transactionTable
                        + " tx where tx.saga_id = b.saga_id) = ? "
                        + "and b.next_retry_time <= current_timestamp(3) "
                        + "and not exists (select 1 from " + outboxTable + " o where o.branch_id = b.id "
                        + "and o.attempts < ? and o.status in ('NEW','FAILED','SENDING') "
                        + "and o.action = ? and o.command_attempt = b." + attemptColumn + ") "
                        + "order by b.next_retry_time, b.id limit ?",
                branchStateMapper, status.name(), transactionStatus.name(), properties.getMaxAttempts(),
                action.name(), limit);
    }

    @Override
    public void scheduleBranchRetry(long branchId, int attempts) {
        jdbcTemplate.update("update " + branchTable + " set next_retry_time = "
                        + "timestampadd(microsecond, ?, current_timestamp(3)), update_time = current_timestamp(3) "
                        + "where id = ?",
                delayMicros(retryDelayMillis(attempts)), branchId);
    }

    public long nextDelayMillis(int attempts) {
        long multiplier = 1L << Math.min(Math.max(attempts, 0), 10);
        long baseDelay = properties.getRetryBaseDelayMillis();
        long delay = baseDelay > Long.MAX_VALUE / multiplier ? Long.MAX_VALUE : baseDelay * multiplier;
        return Math.min(delay, properties.getRetryMaxDelayMillis());
    }

    long retryDelayMillis(int attempts) {
        long delay = nextDelayMillis(attempts);
        int jitterPercent = properties.getRetryJitterPercent();
        if (jitterPercent == 0 || delay <= 1) {
            return delay;
        }
        // 先除后乘，避免合法但很大的延迟值发生溢出，
        // 从而将分钟或小时级的退避时间意外变成 1 毫秒。
        long range = percentageOf(delay, jitterPercent);
        if (range == 0) {
            return delay;
        }
        long lower = Math.max(1L, delay - range);
        long upper = Math.min(properties.getRetryMaxDelayMillis(), safeAdd(delay, range));
        if (lower >= upper) {
            return lower;
        }
        // nextLong 的上界不包含在取值范围内。当运维人员将 Long.MAX_VALUE
        // 配置为重试上限时，需要避免上界加一发生溢出。
        if (upper == Long.MAX_VALUE) {
            return ThreadLocalRandom.current().nextLong(lower, upper);
        }
        return ThreadLocalRandom.current().nextLong(lower, upper + 1L);
    }

    private void appendStatusPlaceholders(StringBuilder sql, List<Object> args,
                                          SagaTccTransactionStatus[] statuses) {
        for (int i = 0; i < statuses.length; i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('?');
            args.add(statuses[i].name());
        }
    }

    private long safeAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private long safeMultiply(long left, long right) {
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private long delayMicros(long delayMillis) {
        return safeMultiply(delayMillis, 1000L);
    }

    private long percentageOf(long value, int percent) {
        return (value / 100L) * percent + (value % 100L) * percent / 100L;
    }

    private String attemptsColumn(SagaTccBranchStatus status) {
        if (status == SagaTccBranchStatus.TRYING) {
            return "try_attempts";
        }
        if (status == SagaTccBranchStatus.CONFIRMING) {
            return "confirm_attempts";
        }
        if (status == SagaTccBranchStatus.CANCELLING) {
            return "cancel_attempts";
        }
        throw new IllegalArgumentException("status is not dispatchable: " + status);
    }

    private SagaTccTransactionStatus transactionStatusFor(SagaTccBranchStatus status) {
        if (status == SagaTccBranchStatus.TRYING) {
            return SagaTccTransactionStatus.TRYING;
        }
        if (status == SagaTccBranchStatus.CONFIRMING) {
            return SagaTccTransactionStatus.COMMITTING;
        }
        if (status == SagaTccBranchStatus.CANCELLING) {
            return SagaTccTransactionStatus.CANCELLING;
        }
        throw new IllegalArgumentException("status is not dispatchable: " + status);
    }

    private SagaTccBranchStatus branchStatusFor(SagaTccAction action) {
        if (action == SagaTccAction.TRY) {
            return SagaTccBranchStatus.TRYING;
        }
        if (action == SagaTccAction.CONFIRM) {
            return SagaTccBranchStatus.CONFIRMING;
        }
        if (action == SagaTccAction.CANCEL) {
            return SagaTccBranchStatus.CANCELLING;
        }
        throw new IllegalArgumentException("outbox action is required");
    }

    private SagaTccAction actionFor(SagaTccBranchStatus status) {
        if (status == SagaTccBranchStatus.TRYING) {
            return SagaTccAction.TRY;
        }
        if (status == SagaTccBranchStatus.CONFIRMING) {
            return SagaTccAction.CONFIRM;
        }
        if (status == SagaTccBranchStatus.CANCELLING) {
            return SagaTccAction.CANCEL;
        }
        throw new IllegalArgumentException("status is not dispatchable: " + status);
    }

    private boolean isDispatchable(SagaTccBranchStatus status) {
        return status == SagaTccBranchStatus.TRYING
                || status == SagaTccBranchStatus.CONFIRMING
                || status == SagaTccBranchStatus.CANCELLING;
    }

    private void discardObsoleteOutbox(long branchId, SagaTccAction action, int commandAttempt) {
        jdbcTemplate.update("update " + outboxTable
                        + " set status = 'DISCARDED', claim_token = null, update_time = current_timestamp(3) "
                        + "where branch_id = ? and status in ('NEW','FAILED','SENDING') "
                        + "and (action is null or action <> ? or command_attempt <> ?)",
                branchId, action.name(), commandAttempt);
    }

    private void discardActiveOutbox(long branchId) {
        jdbcTemplate.update("update " + outboxTable
                        + " set status = 'DISCARDED', claim_token = null, update_time = current_timestamp(3) "
                        + "where branch_id = ? and status in ('NEW','FAILED','SENDING')",
                branchId);
    }

    private String trimError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 2000 ? error.substring(0, 2000) : error;
    }
}
