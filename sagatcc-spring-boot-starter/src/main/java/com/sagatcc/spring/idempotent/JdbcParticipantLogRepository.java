package com.sagatcc.spring.idempotent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.Callable;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.store.SagaTccDataSourceProvider;
import com.sagatcc.spring.store.SagaTccTableNames;

import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcParticipantLogRepository implements ParticipantLogRepository, SagaTccDataSourceProvider {

    private static final String SUCCEEDED = "SUCCEEDED";
    private static final String RUNNING = "RUNNING";

    private final JdbcTemplate jdbcTemplate;
    private final String participantLogTable;

    public JdbcParticipantLogRepository(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, new SagaTccProperties());
    }

    public JdbcParticipantLogRepository(JdbcTemplate jdbcTemplate, SagaTccProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.participantLogTable = new SagaTccTableNames(properties.getSchema()).participantLog();
    }

    @Override
    public javax.sql.DataSource sagaTccDataSource() {
        return jdbcTemplate.getDataSource();
    }

    @Override
    public void executeIdempotently(String localApp, SagaTccCommandMessage command, Callable<Void> businessCall) throws Exception {
        String requestHash = requestHash(command);
        jdbcTemplate.update("insert into " + participantLogTable + " " +
                        "(local_app, coordinator_app, saga_id, branch_id, target_app, bus_code, request_hash, create_time, update_time) " +
                        "values (?, ?, ?, ?, ?, ?, ?, current_timestamp(3), current_timestamp(3)) " +
                        "on duplicate key update id = id",
                localApp, command.getCoordinatorApp(), command.getSagaId(), command.getBranchId(),
                command.getTargetApp(), command.getBusCode(), requestHash);

        Map<String, Object> row = jdbcTemplate.queryForMap("select * from " + participantLogTable + " " +
                        "where local_app = ? and coordinator_app = ? and saga_id = ? and branch_id = ? for update",
                localApp, command.getCoordinatorApp(), command.getSagaId(), command.getBranchId());

        verifyIdentity(row, localApp, command, requestHash);

        if (command.getAction() == SagaTccAction.TRY && SUCCEEDED.equals(row.get("cancel_status"))) {
            return;
        }
        if (command.getAction() == SagaTccAction.CONFIRM && SUCCEEDED.equals(row.get("cancel_status"))) {
            throw new SagaTccNonRetryableException("confirm conflicts with completed cancel, sagaId="
                    + command.getSagaId() + ", branchId=" + command.getBranchId());
        }
        if (command.getAction() == SagaTccAction.CANCEL && SUCCEEDED.equals(row.get("confirm_status"))) {
            throw new SagaTccNonRetryableException("cancel conflicts with completed confirm, sagaId="
                    + command.getSagaId() + ", branchId=" + command.getBranchId());
        }
        if (command.getAction() == SagaTccAction.CANCEL && !SUCCEEDED.equals(row.get("try_status"))) {
            updateStatus(localApp, command, "cancel_status", SUCCEEDED);
            return;
        }
        if (command.getAction() == SagaTccAction.CONFIRM && !SUCCEEDED.equals(row.get("try_status"))) {
            throw new SagaTccException("confirm received before successful try, sagaId=" + command.getSagaId()
                    + ", branchId=" + command.getBranchId());
        }

        String statusColumn = statusColumn(command.getAction());
        if (SUCCEEDED.equals(row.get(statusColumn))) {
            return;
        }

        updateStatus(localApp, command, statusColumn, RUNNING);
        businessCall.call();
        updateStatus(localApp, command, statusColumn, SUCCEEDED);
    }

    private void updateStatus(String localApp, SagaTccCommandMessage command, String column, String status) {
        jdbcTemplate.update("update " + participantLogTable + " set " + column
                        + " = ?, update_time = current_timestamp(3) " +
                        "where local_app = ? and coordinator_app = ? and saga_id = ? and branch_id = ?",
                status, localApp, command.getCoordinatorApp(), command.getSagaId(), command.getBranchId());
    }

    private String statusColumn(SagaTccAction action) {
        if (action == SagaTccAction.TRY) {
            return "try_status";
        }
        if (action == SagaTccAction.CONFIRM) {
            return "confirm_status";
        }
        if (action == SagaTccAction.CANCEL) {
            return "cancel_status";
        }
        throw new SagaTccException("unsupported action: " + action);
    }

    private void verifyIdentity(Map<String, Object> row, String localApp,
                                SagaTccCommandMessage command, String requestHash) {
        if (!equalsValue(row.get("target_app"), command.getTargetApp())
                || !equalsValue(row.get("bus_code"), command.getBusCode())) {
            throw new SagaTccNonRetryableException("participant command identity conflict, sagaId=" + command.getSagaId()
                    + ", branchId=" + command.getBranchId());
        }
        if (equalsValue(row.get("request_hash"), requestHash)) {
            return;
        }
        String json = command.getRequestJson() == null ? "" : command.getRequestJson();
        String legacyHash = Integer.toHexString(json.hashCode());
        if (equalsValue(row.get("request_hash"), legacyHash)) {
            jdbcTemplate.update("update " + participantLogTable
                            + " set request_hash = ?, update_time = current_timestamp(3) "
                            + "where local_app = ? and coordinator_app = ? and saga_id = ? and branch_id = ?",
                    requestHash, localApp, command.getCoordinatorApp(), command.getSagaId(), command.getBranchId());
            return;
        }
        throw new SagaTccNonRetryableException("participant request payload conflict, sagaId=" + command.getSagaId()
                + ", branchId=" + command.getBranchId());
    }

    private boolean equalsValue(Object stored, String expected) {
        return stored != null && stored.toString().equals(expected);
    }

    private String requestHash(SagaTccCommandMessage command) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String value = command.getRequestJson() == null ? "" : command.getRequestJson();
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            char[] hex = new char[bytes.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < bytes.length; i++) {
                int unsigned = bytes[i] & 0xff;
                hex[i * 2] = digits[unsigned >>> 4];
                hex[i * 2 + 1] = digits[unsigned & 0x0f];
            }
            return new String(hex);
        } catch (NoSuchAlgorithmException e) {
            throw new SagaTccException("SHA-256 is not available", e);
        }
    }

}
