package com.sagatcc.spring.idempotent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcParticipantLogRepositoryTest {

    @Test
    void successfulTryWritesRunningAndSucceededAroundTheBusinessCall() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{\"amount\":100}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);
        JdbcParticipantLogRepository repository = new JdbcParticipantLogRepository(jdbc);
        List<String> observedStatuses = new ArrayList<String>();

        repository.executeIdempotently("wallet", command, () -> {
            observedStatuses.add(jdbc.lastStatus("try_status"));
            return null;
        });

        assertEquals("order", jdbc.insertArgument(1));
        assertEquals(sha256(command.getRequestJson()), jdbc.insertArgument(6));
        assertEquals(64, jdbc.insertArgument(6).toString().length());
        assertTrue(jdbc.insertSql().startsWith("insert into saga_tcc_participant_log"));
        assertTrue(jdbc.insertSql().contains("on duplicate key update id = id"));
        assertFalse(jdbc.insertSql().contains("insert ignore"));
        assertEquals("RUNNING", observedStatuses.get(0));
        assertEquals(list("RUNNING", "SUCCEEDED"), jdbc.statuses("try_status"));
    }

    @Test
    void duplicateSuccessfulTryDoesNotInvokeBusinessAgain() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, null);
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("try_status").isEmpty());
        assertEquals(1, jdbc.updates.size(),
                "a duplicate only performs the duplicate-key no-op before locking the row");
    }

    @Test
    void duplicateSuccessfulConfirmDoesNotInvokeBusinessAgain() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.CONFIRM, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", "SUCCEEDED", null);
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("confirm_status").isEmpty());
    }

    @Test
    void duplicateSuccessfulCancelDoesNotInvokeBusinessAgain() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.CANCEL, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, "SUCCEEDED");
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("cancel_status").isEmpty());
    }

    @Test
    void cancelBeforeTryIsAnEmptyRollbackAndCreatesCancelFence() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.CANCEL, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(0, calls.get());
        assertEquals(list("SUCCEEDED"), jdbc.statuses("cancel_status"));
        assertTrue(jdbc.statuses("try_status").isEmpty());
    }

    @Test
    void lateTryAfterEmptyRollbackIsFencedOut() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, "SUCCEEDED");
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("try_status").isEmpty());
    }

    @Test
    void confirmBeforeSuccessfulTryIsRejectedWithoutCallingBusiness() {
        SagaTccCommandMessage command = command(SagaTccAction.CONFIRM, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);
        AtomicInteger calls = new AtomicInteger();

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
                    calls.incrementAndGet();
                    return null;
                }));

        assertTrue(failure.getMessage().contains("confirm received before successful try"));
        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("confirm_status").isEmpty());
    }

    @Test
    void confirmAfterTryTransitionsIndependently() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.CONFIRM, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, null);
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(1, calls.get());
        assertEquals(list("RUNNING", "SUCCEEDED"), jdbc.statuses("confirm_status"));
        assertTrue(jdbc.statuses("try_status").isEmpty());
    }

    @Test
    void cancelAfterTryInvokesCompensationExactlyOnce() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.CANCEL, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, null);
        AtomicInteger calls = new AtomicInteger();

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
            calls.incrementAndGet();
            return null;
        });

        assertEquals(1, calls.get());
        assertEquals(list("RUNNING", "SUCCEEDED"), jdbc.statuses("cancel_status"));
    }

    @Test
    void lateConfirmAfterCompletedCancelIsAPermanentProtocolConflict() {
        SagaTccCommandMessage command = command(SagaTccAction.CONFIRM, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, "SUCCEEDED");
        AtomicInteger calls = new AtomicInteger();

        SagaTccNonRetryableException failure = assertThrows(SagaTccNonRetryableException.class,
                () -> new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
                    calls.incrementAndGet();
                    return null;
                }));

        assertTrue(failure.getMessage().contains("confirm conflicts with completed cancel"));
        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("confirm_status").isEmpty());
    }

    @Test
    void lateCancelAfterCompletedConfirmIsAPermanentProtocolConflict() {
        SagaTccCommandMessage command = command(SagaTccAction.CANCEL, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", "SUCCEEDED", null);
        AtomicInteger calls = new AtomicInteger();

        SagaTccNonRetryableException failure = assertThrows(SagaTccNonRetryableException.class,
                () -> new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
                    calls.incrementAndGet();
                    return null;
                }));

        assertTrue(failure.getMessage().contains("cancel conflicts with completed confirm"));
        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("cancel_status").isEmpty());
    }

    @Test
    void coordinatorApplicationIsPartOfEveryPersistenceKey() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> null);

        assertTrue(jdbc.insertSql().contains("coordinator_app"));
        assertEquals("order", jdbc.insertArgument(1));
        assertEquals(1, jdbc.queries.size());
        assertTrue(jdbc.queries.get(0).sql.contains("coordinator_app = ?"));
        assertEquals("order", jdbc.queries.get(0).args[1]);
        for (Update update : jdbc.updates) {
            if (!update.sql.startsWith("insert into saga_tcc_participant_log")) {
                assertTrue(update.sql.contains("coordinator_app"), update.sql);
            }
        }
    }

    @Test
    void differentPayloadForTheSameBranchIsRejectedBeforeStateChange() {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{\"amount\":101}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);
        jdbc.row.put("request_hash", sha256("{\"amount\":100}"));
        AtomicInteger calls = new AtomicInteger();

        SagaTccNonRetryableException failure = assertThrows(SagaTccNonRetryableException.class,
                () -> new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
                    calls.incrementAndGet();
                    return null;
                }));

        assertTrue(failure.getMessage().contains("payload conflict"));
        assertEquals(0, calls.get());
        assertTrue(jdbc.statuses("try_status").isEmpty());
    }

    @Test
    void differentTargetOrBusinessCodeForTheSameBranchIsRejected() {
        SagaTccCommandMessage changedTarget = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate targetJdbc = jdbcRow(changedTarget, null, null, null);
        targetJdbc.row.put("target_app", "another-wallet");

        SagaTccNonRetryableException targetFailure = assertThrows(SagaTccNonRetryableException.class,
                () -> new JdbcParticipantLogRepository(targetJdbc)
                        .executeIdempotently("wallet", changedTarget, () -> null));

        SagaTccCommandMessage changedBusiness = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate businessJdbc = jdbcRow(changedBusiness, null, null, null);
        businessJdbc.row.put("bus_code", "refund");
        SagaTccNonRetryableException businessFailure = assertThrows(SagaTccNonRetryableException.class,
                () -> new JdbcParticipantLogRepository(businessJdbc)
                        .executeIdempotently("wallet", changedBusiness, () -> null));

        assertTrue(targetFailure.getMessage().contains("identity conflict"));
        assertTrue(businessFailure.getMessage().contains("identity conflict"));
        assertTrue(targetJdbc.statuses("try_status").isEmpty());
        assertTrue(businessJdbc.statuses("try_status").isEmpty());
    }

    @Test
    void legacyJavaHashIsUpgradedToSha256BeforeDuplicateCheckCompletes() throws Exception {
        String json = "{\"legacy\":true}";
        SagaTccCommandMessage command = command(SagaTccAction.TRY, json);
        RecordingJdbcTemplate jdbc = jdbcRow(command, "SUCCEEDED", null, null);
        jdbc.row.put("request_hash", Integer.toHexString(json.hashCode()));

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command,
                () -> {
                    throw new AssertionError("a completed legacy request must stay idempotent");
                });

        assertEquals(list(sha256(json)), jdbc.statuses("request_hash"));
        assertTrue(jdbc.statuses("try_status").isEmpty());
    }

    @Test
    void nullPayloadHasStableEmptyStringHash() throws Exception {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, null);
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);

        new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> null);

        assertEquals(sha256(""), jdbc.insertArgument(6));
    }

    @Test
    void businessFailureNeverWritesSucceeded() {
        SagaTccCommandMessage command = command(SagaTccAction.TRY, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);
        IllegalStateException expected = new IllegalStateException("insufficient balance");

        IllegalStateException actual = assertThrows(IllegalStateException.class,
                () -> new JdbcParticipantLogRepository(jdbc).executeIdempotently("wallet", command, () -> {
                    throw expected;
                }));

        assertEquals(expected, actual);
        assertEquals(list("RUNNING"), jdbc.statuses("try_status"));
        assertFalse(jdbc.statuses("try_status").contains("SUCCEEDED"));
    }

    @Test
    void unsupportedNullActionIsRejected() {
        SagaTccCommandMessage command = command(null, "{}");
        RecordingJdbcTemplate jdbc = jdbcRow(command, null, null, null);

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> new JdbcParticipantLogRepository(jdbc)
                        .executeIdempotently("wallet", command, () -> null));

        assertTrue(failure.getMessage().contains("unsupported action"));
    }

    private static RecordingJdbcTemplate jdbcRow(SagaTccCommandMessage command, String tryStatus,
                                                   String confirmStatus, String cancelStatus) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("target_app", command.getTargetApp());
        row.put("coordinator_app", command.getCoordinatorApp());
        row.put("bus_code", command.getBusCode());
        row.put("request_hash", sha256(command.getRequestJson() == null ? "" : command.getRequestJson()));
        row.put("try_status", tryStatus);
        row.put("confirm_status", confirmStatus);
        row.put("cancel_status", cancelStatus);
        return new RecordingJdbcTemplate(row);
    }

    private static SagaTccCommandMessage command(SagaTccAction action, String requestJson) {
        SagaTccCommandMessage command = new SagaTccCommandMessage();
        command.setMessageKey("saga-1-" + action + "-1");
        command.setSagaId("saga-1");
        command.setBranchId(9L);
        command.setCoordinatorApp("order");
        command.setTargetApp("wallet");
        command.setBusCode("pay");
        command.setAction(action);
        command.setRequestClass("example.PayRequest");
        command.setRequestJson(requestJson);
        command.setAttempt(1);
        return command;
    }

    private static String sha256(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                hex.append(String.format("%02x", current & 0xff));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static List<String> list(String... values) {
        List<String> result = new ArrayList<String>();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }

    private static final class RecordingJdbcTemplate extends JdbcTemplate {

        private final Map<String, Object> row;
        private final List<Update> updates = new ArrayList<Update>();
        private final List<Update> queries = new ArrayList<Update>();

        private RecordingJdbcTemplate(Map<String, Object> row) {
            this.row = row;
        }

        @Override
        public int update(String sql, Object... args) {
            updates.add(new Update(sql, args.clone()));
            if (sql.startsWith("update saga_tcc_participant_log set ")) {
                String column = sql.substring("update saga_tcc_participant_log set ".length(), sql.indexOf(" = ?"));
                row.put(column, args[0]);
            }
            return 1;
        }

        @Override
        public Map<String, Object> queryForMap(String sql, Object... args) {
            queries.add(new Update(sql, args.clone()));
            return row;
        }

        private Object insertArgument(int index) {
            for (Update update : updates) {
                if (update.sql.startsWith("insert into saga_tcc_participant_log")) {
                    return update.args[index];
                }
            }
            throw new AssertionError("insert was not recorded");
        }

        private String insertSql() {
            for (Update update : updates) {
                if (update.sql.startsWith("insert into saga_tcc_participant_log")) {
                    return update.sql;
                }
            }
            throw new AssertionError("insert was not recorded");
        }

        private List<String> statuses(String column) {
            List<String> result = new ArrayList<String>();
            String prefix = "update saga_tcc_participant_log set " + column + " = ?";
            for (Update update : updates) {
                if (update.sql.startsWith(prefix)) {
                    result.add(String.valueOf(update.args[0]));
                }
            }
            return result;
        }

        private String lastStatus(String column) {
            List<String> values = statuses(column);
            return values.isEmpty() ? null : values.get(values.size() - 1);
        }
    }

    private static final class Update {

        private final String sql;
        private final Object[] args;

        private Update(String sql, Object[] args) {
            this.sql = sql;
            this.args = args;
        }
    }
}
