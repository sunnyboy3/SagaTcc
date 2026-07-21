package com.sagatcc.spring.config;

import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaTccPropertiesTest {

    @Test
    void defaultConfigurationIsInternallyConsistent() {
        SagaTccProperties properties = new SagaTccProperties();

        assertAll(
                () -> assertDoesNotThrow(properties::afterPropertiesSet),
                () -> assertEquals("transactionManager", properties.getTransactionManagerBeanName()),
                () -> assertEquals(16, properties.getMaxAttempts()),
                () -> assertEquals(1000L, properties.getRetryBaseDelayMillis()),
                () -> assertEquals(60000L, properties.getRetryMaxDelayMillis()),
                () -> assertEquals(20, properties.getRetryJitterPercent()),
                () -> assertEquals(100, properties.getScanBatchSize()),
                () -> assertEquals(1000, properties.getMaxBranchesPerSaga()),
                () -> assertEquals(1048576, properties.getMaxRequestBytes()),
                () -> assertEquals(10485760, properties.getMaxSagaPayloadBytes()),
                () -> assertEquals(4194304, properties.getMaxMessageBytes()),
                () -> assertEquals(20, properties.getOutboxClaimBatchSize()),
                () -> assertEquals(4, properties.getOutboxPublishConcurrency()),
                () -> assertEquals(30000L, properties.getOutboxClaimTimeoutMillis()),
                () -> assertTrue(properties.isSchedulerEnabled()),
                () -> assertFalse(properties.getRocketmq().isPerApplicationTopic()),
                () -> assertEquals(3000L, properties.getRocketmq().getSendTimeoutMillis()));
    }

    @Test
    void everyStrictlyPositiveNumericPropertyRejectsZero() {
        assertAll(
                () -> rejects("max-attempts", p -> p.setMaxAttempts(0)),
                () -> rejects("retry-base-delay-millis", p -> p.setRetryBaseDelayMillis(0)),
                () -> rejects("retry-max-delay-millis", p -> p.setRetryMaxDelayMillis(0)),
                () -> rejects("scan-batch-size", p -> p.setScanBatchSize(0)),
                () -> rejects("max-branches-per-saga", p -> p.setMaxBranchesPerSaga(0)),
                () -> rejects("max-request-bytes", p -> p.setMaxRequestBytes(0)),
                () -> rejects("max-saga-payload-bytes", p -> p.setMaxSagaPayloadBytes(0)),
                () -> rejects("max-message-bytes", p -> p.setMaxMessageBytes(0)),
                () -> rejects("outbox-claim-batch-size", p -> p.setOutboxClaimBatchSize(0)),
                () -> rejects("outbox-publish-concurrency", p -> p.setOutboxPublishConcurrency(0)),
                () -> rejects("outbox-claim-timeout-millis", p -> p.setOutboxClaimTimeoutMillis(0)),
                () -> rejects("rocketmq.send-timeout-millis", p -> p.getRocketmq().setSendTimeoutMillis(0)));
    }

    @Test
    void everyStrictlyPositiveNumericPropertyAcceptsItsEffectiveMinimum() {
        assertAll(
                () -> accepts(p -> p.setMaxAttempts(1)),
                () -> accepts(p -> p.setRetryBaseDelayMillis(1)),
                () -> accepts(p -> {
                    p.setRetryBaseDelayMillis(1);
                    p.setRetryMaxDelayMillis(1);
                }),
                () -> accepts(p -> p.setScanBatchSize(1)),
                () -> accepts(p -> p.setMaxBranchesPerSaga(1)),
                () -> accepts(p -> {
                    p.setMaxRequestBytes(1);
                    p.setMaxSagaPayloadBytes(1);
                }),
                () -> accepts(p -> p.setOutboxClaimBatchSize(1)),
                () -> accepts(p -> p.setOutboxPublishConcurrency(1)),
                () -> accepts(p -> {
                    p.getRocketmq().setSendTimeoutMillis(1);
                    p.setOutboxClaimTimeoutMillis(2);
                }));
    }

    @Test
    void retryMaximumAcceptsEqualityAndRejectsOneMillisecondBelowBase() {
        accepts(p -> {
            p.setRetryBaseDelayMillis(2000);
            p.setRetryMaxDelayMillis(2000);
        });

        rejects("greater than or equal", p -> {
            p.setRetryBaseDelayMillis(2000);
            p.setRetryMaxDelayMillis(1999);
        });
    }

    @Test
    void retryJitterAcceptsBothBoundsAndRejectsOneBeyondEitherBound() {
        assertAll(
                () -> accepts(p -> p.setRetryJitterPercent(0)),
                () -> accepts(p -> p.setRetryJitterPercent(50)),
                () -> rejects("between 0 and 50", p -> p.setRetryJitterPercent(-1)),
                () -> rejects("between 0 and 50", p -> p.setRetryJitterPercent(51)));
    }

    @Test
    void sagaPayloadLimitAcceptsRequestLimitAndRejectsOneByteLess() {
        accepts(p -> {
            p.setMaxRequestBytes(4096);
            p.setMaxSagaPayloadBytes(4096);
        });

        rejects("greater than or equal", p -> {
            p.setMaxRequestBytes(4096);
            p.setMaxSagaPayloadBytes(4095);
        });
    }

    @Test
    void messageLimitReservesTheExactWorstCaseCommandEnvelopeBoundary() {
        accepts(p -> {
            p.setMaxRequestBytes(100);
            p.setMaxSagaPayloadBytes(100);
            p.setMaxMessageBytes(2248);
        });
        rejects("command envelope", p -> {
            p.setMaxRequestBytes(100);
            p.setMaxSagaPayloadBytes(100);
            p.setMaxMessageBytes(2247);
        });
    }

    @Test
    void operationalBatchAndBranchCapsAcceptLimitAndRejectLimitPlusOne() {
        assertAll(
                () -> accepts(p -> p.setScanBatchSize(10000)),
                () -> rejects("scan-batch-size must not exceed", p -> p.setScanBatchSize(10001)),
                () -> accepts(p -> p.setMaxBranchesPerSaga(10000)),
                () -> rejects("max-branches-per-saga must not exceed", p -> p.setMaxBranchesPerSaga(10001)),
                () -> accepts(p -> p.setOutboxClaimBatchSize(1000)),
                () -> rejects("outbox-claim-batch-size must not exceed",
                        p -> p.setOutboxClaimBatchSize(1001)));
    }

    @Test
    void transactionManagerNameMustNotBeBlank() {
        rejects("transaction-manager-bean-name", p -> p.setTransactionManagerBeanName(" \t"));
    }

    @Test
    void publisherConcurrencyAccepts64AndRejects65() {
        assertAll(
                () -> accepts(p -> p.setOutboxPublishConcurrency(64)),
                () -> rejects("must not exceed 64", p -> p.setOutboxPublishConcurrency(65)));
    }

    @Test
    void claimLeaseMustExceedSendTimeoutByAtLeastOneMillisecond() {
        accepts(p -> {
            p.getRocketmq().setSendTimeoutMillis(3000);
            p.setOutboxClaimTimeoutMillis(3001);
        });

        rejects("must be greater than rocketmq.send-timeout-millis", p -> {
            p.getRocketmq().setSendTimeoutMillis(3000);
            p.setOutboxClaimTimeoutMillis(3000);
        });
    }

    @Test
    void databaseSchedulingDelayRejectsValuesBeyondOneYearWithoutOverflowing() {
        rejects("retry-base-delay-millis must not exceed", p -> {
            p.setRetryBaseDelayMillis(Long.MAX_VALUE);
            p.setRetryMaxDelayMillis(Long.MAX_VALUE);
        });
        rejects("outbox-claim-timeout-millis must not exceed",
                p -> p.setOutboxClaimTimeoutMillis(Long.MAX_VALUE));
    }

    @Test
    void sharedTopicsAccept127AndReject128Characters() {
        accepts(p -> {
            p.getRocketmq().setCommandTopic(text(127));
            p.getRocketmq().setResultTopic(text(127));
        });

        assertAll(
                () -> rejects("command-topic", p -> p.getRocketmq().setCommandTopic(text(128))),
                () -> rejects("result-topic", p -> p.getRocketmq().setResultTopic(text(128))));
    }

    @Test
    void sharedTopicsRejectNullBlankAndColon() {
        assertAll(
                () -> rejects("command-topic", p -> p.getRocketmq().setCommandTopic(null)),
                () -> rejects("command-topic", p -> p.getRocketmq().setCommandTopic(" \t")),
                () -> rejects("command-topic", p -> p.getRocketmq().setCommandTopic("bad:topic")),
                () -> rejects("command-topic", p -> p.getRocketmq().setCommandTopic("bad.topic")),
                () -> rejects("result-topic", p -> p.getRocketmq().setResultTopic(null)),
                () -> rejects("result-topic", p -> p.getRocketmq().setResultTopic(" \n")),
                () -> rejects("result-topic", p -> p.getRocketmq().setResultTopic("bad:topic")),
                () -> rejects("result-topic", p -> p.getRocketmq().setResultTopic("bad/topic")));
    }

    @Test
    void perApplicationPrefixesAreValidatedOnlyWhenFeatureIsEnabled() {
        accepts(p -> {
            p.getRocketmq().setPerApplicationTopic(false);
            p.getRocketmq().setCommandTopicPrefix(null);
            p.getRocketmq().setResultTopicPrefix(":");
        });

        assertAll(
                () -> rejects("command-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setCommandTopicPrefix(null);
                }),
                () -> rejects("result-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setResultTopicPrefix(" ");
                }),
                () -> rejects("command-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setCommandTopicPrefix("bad:");
                }),
                () -> rejects("result-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setResultTopicPrefix("bad:");
                }));
    }

    @Test
    void perApplicationPrefixesAccept127AndReject128Characters() {
        accepts(p -> {
            p.getRocketmq().setPerApplicationTopic(true);
            p.getRocketmq().setCommandTopicPrefix(text(127));
            p.getRocketmq().setResultTopicPrefix(text(127));
        });

        assertAll(
                () -> rejects("command-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setCommandTopicPrefix(text(128));
                }),
                () -> rejects("result-topic-prefix", p -> {
                    p.getRocketmq().setPerApplicationTopic(true);
                    p.getRocketmq().setResultTopicPrefix(text(128));
                }));
    }

    private static void accepts(Consumer<SagaTccProperties> mutation) {
        SagaTccProperties properties = new SagaTccProperties();
        mutation.accept(properties);
        assertDoesNotThrow(properties::afterPropertiesSet);
    }

    private static void rejects(String messageFragment, Consumer<SagaTccProperties> mutation) {
        SagaTccProperties properties = new SagaTccProperties();
        mutation.accept(properties);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, properties::afterPropertiesSet);
        assertTrue(error.getMessage().contains(messageFragment), error::getMessage);
    }

    private static String text(int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }
}
