package com.sagatcc.spring.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.beans.factory.InitializingBean;

import com.sagatcc.spring.store.SagaTccTableNames;

@ConfigurationProperties(prefix = "sagatcc")
public class SagaTccProperties implements InitializingBean {

    private static final long MAX_DATABASE_DELAY_MILLIS = 365L * 24L * 60L * 60L * 1000L;

    private String applicationName;
    private String transactionManagerBeanName = "transactionManager";
    private String schema;
    private SagaTccBranchExecutionMode branchExecutionMode = SagaTccBranchExecutionMode.PARALLEL;
    private final Map<String, SagaTccBranchExecutionMode> branchExecutionModes =
            new LinkedHashMap<String, SagaTccBranchExecutionMode>();
    private int maxAttempts = 16;
    private long retryBaseDelayMillis = 1000L;
    private long retryMaxDelayMillis = 60000L;
    private int retryJitterPercent = 20;
    private int scanBatchSize = 100;
    private int maxBranchesPerSaga = 1000;
    private int maxRequestBytes = 1048576;
    private int maxSagaPayloadBytes = 10485760;
    private int maxMessageBytes = 4194304;
    private int outboxClaimBatchSize = 20;
    private int outboxPublishConcurrency = 4;
    private long outboxClaimTimeoutMillis = 30000L;
    private boolean schedulerEnabled = true;
    private final Rocketmq rocketmq = new Rocketmq();

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public String getTransactionManagerBeanName() {
        return transactionManagerBeanName;
    }

    public void setTransactionManagerBeanName(String transactionManagerBeanName) {
        this.transactionManagerBeanName = transactionManagerBeanName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public SagaTccBranchExecutionMode getBranchExecutionMode() {
        return branchExecutionMode;
    }

    public void setBranchExecutionMode(SagaTccBranchExecutionMode branchExecutionMode) {
        this.branchExecutionMode = branchExecutionMode;
    }

    /**
     * 按事务业务编码覆盖默认分支调度模式。
     */
    public Map<String, SagaTccBranchExecutionMode> getBranchExecutionModes() {
        return branchExecutionModes;
    }

    public SagaTccBranchExecutionMode resolveBranchExecutionMode(String businessCode) {
        SagaTccBranchExecutionMode configured = businessCode == null
                ? null : branchExecutionModes.get(businessCode);
        return configured == null ? branchExecutionMode : configured;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getRetryBaseDelayMillis() {
        return retryBaseDelayMillis;
    }

    public void setRetryBaseDelayMillis(long retryBaseDelayMillis) {
        this.retryBaseDelayMillis = retryBaseDelayMillis;
    }

    public long getRetryMaxDelayMillis() {
        return retryMaxDelayMillis;
    }

    public void setRetryMaxDelayMillis(long retryMaxDelayMillis) {
        this.retryMaxDelayMillis = retryMaxDelayMillis;
    }

    public int getRetryJitterPercent() {
        return retryJitterPercent;
    }

    public void setRetryJitterPercent(int retryJitterPercent) {
        this.retryJitterPercent = retryJitterPercent;
    }

    public int getScanBatchSize() {
        return scanBatchSize;
    }

    public void setScanBatchSize(int scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }

    public long getOutboxClaimTimeoutMillis() {
        return outboxClaimTimeoutMillis;
    }

    public void setOutboxClaimTimeoutMillis(long outboxClaimTimeoutMillis) {
        this.outboxClaimTimeoutMillis = outboxClaimTimeoutMillis;
    }

    public int getMaxBranchesPerSaga() {
        return maxBranchesPerSaga;
    }

    public void setMaxBranchesPerSaga(int maxBranchesPerSaga) {
        this.maxBranchesPerSaga = maxBranchesPerSaga;
    }

    public int getMaxRequestBytes() {
        return maxRequestBytes;
    }

    public void setMaxRequestBytes(int maxRequestBytes) {
        this.maxRequestBytes = maxRequestBytes;
    }

    public int getMaxSagaPayloadBytes() {
        return maxSagaPayloadBytes;
    }

    public int getMaxMessageBytes() {
        return maxMessageBytes;
    }

    public void setMaxMessageBytes(int maxMessageBytes) {
        this.maxMessageBytes = maxMessageBytes;
    }

    public void setMaxSagaPayloadBytes(int maxSagaPayloadBytes) {
        this.maxSagaPayloadBytes = maxSagaPayloadBytes;
    }

    public int getOutboxClaimBatchSize() {
        return outboxClaimBatchSize;
    }

    public void setOutboxClaimBatchSize(int outboxClaimBatchSize) {
        this.outboxClaimBatchSize = outboxClaimBatchSize;
    }

    public int getOutboxPublishConcurrency() {
        return outboxPublishConcurrency;
    }

    public void setOutboxPublishConcurrency(int outboxPublishConcurrency) {
        this.outboxPublishConcurrency = outboxPublishConcurrency;
    }

    public boolean isSchedulerEnabled() {
        return schedulerEnabled;
    }

    public void setSchedulerEnabled(boolean schedulerEnabled) {
        this.schedulerEnabled = schedulerEnabled;
    }

    public Rocketmq getRocketmq() {
        return rocketmq;
    }

    @Override
    public void afterPropertiesSet() {
        schema = SagaTccTableNames.normalizeSchema(schema);
        requireText(transactionManagerBeanName, "transaction-manager-bean-name");
        if (branchExecutionMode == null) {
            throw new IllegalArgumentException("sagatcc.branch-execution-mode must not be null");
        }
        for (Map.Entry<String, SagaTccBranchExecutionMode> entry : branchExecutionModes.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().length() == 0) {
                throw new IllegalArgumentException("sagatcc.branch-execution-modes business code must not be blank");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("sagatcc.branch-execution-modes["
                        + entry.getKey() + "] must not be null");
            }
        }
        requirePositive(maxAttempts, "max-attempts");
        requirePositive(retryBaseDelayMillis, "retry-base-delay-millis");
        requirePositive(retryMaxDelayMillis, "retry-max-delay-millis");
        requireAtMost(retryBaseDelayMillis, MAX_DATABASE_DELAY_MILLIS, "retry-base-delay-millis");
        requireAtMost(retryMaxDelayMillis, MAX_DATABASE_DELAY_MILLIS, "retry-max-delay-millis");
        if (retryMaxDelayMillis < retryBaseDelayMillis) {
            throw new IllegalArgumentException("sagatcc.retry-max-delay-millis must be greater than or equal to retry-base-delay-millis");
        }
        if (retryJitterPercent < 0 || retryJitterPercent > 50) {
            throw new IllegalArgumentException("sagatcc.retry-jitter-percent must be between 0 and 50");
        }
        requirePositive(scanBatchSize, "scan-batch-size");
        requireAtMost(scanBatchSize, 10000, "scan-batch-size");
        requirePositive(maxBranchesPerSaga, "max-branches-per-saga");
        requireAtMost(maxBranchesPerSaga, 10000, "max-branches-per-saga");
        requirePositive(maxRequestBytes, "max-request-bytes");
        requirePositive(maxSagaPayloadBytes, "max-saga-payload-bytes");
        if (maxSagaPayloadBytes < maxRequestBytes) {
            throw new IllegalArgumentException("sagatcc.max-saga-payload-bytes must be greater than or equal to max-request-bytes");
        }
        requirePositive(maxMessageBytes, "max-message-bytes");
        if ((long) maxRequestBytes * 2L + 2048L > maxMessageBytes) {
            throw new IllegalArgumentException("sagatcc.max-message-bytes must leave room for the serialized "
                    + "command envelope (at least max-request-bytes * 2 + 2048)");
        }
        requirePositive(outboxClaimBatchSize, "outbox-claim-batch-size");
        requireAtMost(outboxClaimBatchSize, 1000, "outbox-claim-batch-size");
        requirePositive(outboxPublishConcurrency, "outbox-publish-concurrency");
        if (outboxPublishConcurrency > 64) {
            throw new IllegalArgumentException("sagatcc.outbox-publish-concurrency must not exceed 64");
        }
        requirePositive(outboxClaimTimeoutMillis, "outbox-claim-timeout-millis");
        requireAtMost(outboxClaimTimeoutMillis, MAX_DATABASE_DELAY_MILLIS, "outbox-claim-timeout-millis");
        requirePositive(rocketmq.sendTimeoutMillis, "rocketmq.send-timeout-millis");
        requireAtMost(rocketmq.sendTimeoutMillis, MAX_DATABASE_DELAY_MILLIS, "rocketmq.send-timeout-millis");
        if (rocketmq.sendTimeoutMillis > (MAX_DATABASE_DELAY_MILLIS - 1000L) / outboxClaimBatchSize) {
            throw new IllegalArgumentException("sagatcc.rocketmq.send-timeout-millis multiplied by "
                    + "outbox-claim-batch-size must not exceed the database scheduling range");
        }
        if (outboxClaimTimeoutMillis <= rocketmq.sendTimeoutMillis) {
            throw new IllegalArgumentException("sagatcc.outbox-claim-timeout-millis must be greater than rocketmq.send-timeout-millis");
        }
        requireTopicText(rocketmq.commandTopic, "rocketmq.command-topic", 127);
        requireTopicText(rocketmq.resultTopic, "rocketmq.result-topic", 127);
        if (rocketmq.perApplicationTopic) {
            requireTopicText(rocketmq.commandTopicPrefix, "rocketmq.command-topic-prefix", 127);
            requireTopicText(rocketmq.resultTopicPrefix, "rocketmq.result-topic-prefix", 127);
        }
    }

    private void requirePositive(long value, String property) {
        if (value <= 0) {
            throw new IllegalArgumentException("sagatcc." + property + " must be greater than zero");
        }
    }

    private void requireAtMost(long value, long maximum, String property) {
        if (value > maximum) {
            throw new IllegalArgumentException("sagatcc." + property + " must not exceed " + maximum);
        }
    }

    private void requireText(String value, String property) {
        if (value == null || value.trim().length() == 0) {
            throw new IllegalArgumentException("sagatcc." + property + " must not be blank");
        }
    }

    private void requireTopicText(String value, String property, int maxLength) {
        requireText(value, property);
        if (value.length() > maxLength || !SagaTccNameResolver.isRocketMqName(value)) {
            throw new IllegalArgumentException("sagatcc." + property + " must contain only RocketMQ-safe "
                    + "characters [%|a-zA-Z0-9_-] and not exceed " + maxLength + " characters");
        }
    }

    public static class Rocketmq {

        private boolean perApplicationTopic;
        private String commandTopic = "sagatcc-command";
        private String resultTopic = "sagatcc-result";
        private String commandTopicPrefix = "sagatcc-command-";
        private String resultTopicPrefix = "sagatcc-result-";
        private long sendTimeoutMillis = 3000L;

        public boolean isPerApplicationTopic() {
            return perApplicationTopic;
        }

        public void setPerApplicationTopic(boolean perApplicationTopic) {
            this.perApplicationTopic = perApplicationTopic;
        }

        public String getCommandTopic() {
            return commandTopic;
        }

        public void setCommandTopic(String commandTopic) {
            this.commandTopic = commandTopic;
        }

        public String getResultTopic() {
            return resultTopic;
        }

        public void setResultTopic(String resultTopic) {
            this.resultTopic = resultTopic;
        }

        public String getCommandTopicPrefix() {
            return commandTopicPrefix;
        }

        public void setCommandTopicPrefix(String commandTopicPrefix) {
            this.commandTopicPrefix = commandTopicPrefix;
        }

        public String getResultTopicPrefix() {
            return resultTopicPrefix;
        }

        public void setResultTopicPrefix(String resultTopicPrefix) {
            this.resultTopicPrefix = resultTopicPrefix;
        }

        public long getSendTimeoutMillis() {
            return sendTimeoutMillis;
        }

        public void setSendTimeoutMillis(long sendTimeoutMillis) {
            this.sendTimeoutMillis = sendTimeoutMillis;
        }
    }
}
