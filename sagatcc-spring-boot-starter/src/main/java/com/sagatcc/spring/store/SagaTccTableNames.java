package com.sagatcc.spring.store;

/** 根据可选的 MySQL Schema 生成安全的 SagaTcc 限定表名。 */
public final class SagaTccTableNames {

    private static final int MYSQL_IDENTIFIER_MAX_LENGTH = 64;

    private final String transaction;
    private final String branch;
    private final String outbox;
    private final String participantLog;

    public SagaTccTableNames(String schema) {
        String normalizedSchema = normalizeSchema(schema);
        this.transaction = qualify(normalizedSchema, "saga_tcc_transaction");
        this.branch = qualify(normalizedSchema, "saga_tcc_branch");
        this.outbox = qualify(normalizedSchema, "saga_tcc_outbox");
        this.participantLog = qualify(normalizedSchema, "saga_tcc_participant_log");
    }

    public String transaction() {
        return transaction;
    }

    public String branch() {
        return branch;
    }

    public String outbox() {
        return outbox;
    }

    public String participantLog() {
        return participantLog;
    }

    public static String normalizeSchema(String schema) {
        if (schema == null || schema.trim().length() == 0) {
            return null;
        }
        String normalized = schema.trim();
        if (normalized.length() > MYSQL_IDENTIFIER_MAX_LENGTH) {
            throw new IllegalArgumentException("sagatcc.schema must not exceed 64 characters");
        }
        for (int i = 0; i < normalized.length(); i++) {
            char current = normalized.charAt(i);
            if (!(current == '_'
                    || current >= 'a' && current <= 'z'
                    || current >= 'A' && current <= 'Z'
                    || current >= '0' && current <= '9')) {
                throw new IllegalArgumentException(
                        "sagatcc.schema must contain only letters, digits, and underscores");
            }
        }
        return normalized;
    }

    private String qualify(String schema, String table) {
        return schema == null ? table : "`" + schema + "`.`" + table + "`";
    }
}
