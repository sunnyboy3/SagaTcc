package com.sagatcc.spring.store;

import com.sagatcc.spring.config.SagaTccProperties;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JdbcSagaTccRepositoryBackoffTest {

    @Test
    void exponentialBackoffHandlesNegativeAndHugeAttemptNumbersWithoutShiftOverflow() {
        JdbcSagaTccRepository repository = repository(5L, Long.MAX_VALUE, 0);

        assertAll(
                () -> assertEquals(5L, repository.nextDelayMillis(Integer.MIN_VALUE)),
                () -> assertEquals(5L, repository.nextDelayMillis(-1)),
                () -> assertEquals(5L, repository.nextDelayMillis(0)),
                () -> assertEquals(10L, repository.nextDelayMillis(1)),
                () -> assertEquals(2560L, repository.nextDelayMillis(9)),
                () -> assertEquals(5120L, repository.nextDelayMillis(10)),
                () -> assertEquals(5120L, repository.nextDelayMillis(11)),
                () -> assertEquals(5120L, repository.nextDelayMillis(Integer.MAX_VALUE)));
    }

    @Test
    void exponentialBackoffSaturatesInsteadOfWrappingAtLongMaximum() {
        long overflowingBase = Long.MAX_VALUE / 2L + 1L;
        JdbcSagaTccRepository repository = repository(overflowingBase, Long.MAX_VALUE, 0);

        assertAll(
                () -> assertEquals(overflowingBase, repository.nextDelayMillis(0)),
                () -> assertEquals(Long.MAX_VALUE, repository.nextDelayMillis(1)),
                () -> assertEquals(Long.MAX_VALUE, repository.nextDelayMillis(Integer.MAX_VALUE)));
    }

    @Test
    void maximumDelayCapsBeforeAndAfterExponentSaturation() {
        JdbcSagaTccRepository repository = repository(800L, 1000L, 0);

        assertAll(
                () -> assertEquals(800L, repository.nextDelayMillis(0)),
                () -> assertEquals(1000L, repository.nextDelayMillis(1)),
                () -> assertEquals(1000L, repository.nextDelayMillis(10)),
                () -> assertEquals(1000L, repository.nextDelayMillis(Integer.MAX_VALUE)));
    }

    @Test
    void zeroJitterAndSubMillisecondPercentageRangeAreDeterministic() {
        JdbcSagaTccRepository noJitter = repository(1000L, 10000L, 0);
        JdbcSagaTccRepository roundedToZero = repository(4L, 100L, 20);

        for (int i = 0; i < 1000; i++) {
            assertEquals(2000L, noJitter.retryDelayMillis(1));
            assertEquals(4L, roundedToZero.retryDelayMillis(0));
        }
    }

    @Test
    void jitterAlwaysStaysInsideInclusiveConfiguredPercentageBounds() {
        JdbcSagaTccRepository repository = repository(1000L, 10000L, 20);
        long minimum = Long.MAX_VALUE;
        long maximum = Long.MIN_VALUE;

        for (int i = 0; i < 10000; i++) {
            long delay = repository.retryDelayMillis(0);
            assertTrue(delay >= 800L && delay <= 1200L, "delay=" + delay);
            minimum = Math.min(minimum, delay);
            maximum = Math.max(maximum, delay);
        }

        assertTrue(minimum < maximum, "jitter must not collapse to a constant");
    }

    @Test
    void jitterNeverExceedsRetryCapWhenBaseDelayIsAlreadyCapped() {
        JdbcSagaTccRepository repository = repository(800L, 1000L, 50);

        for (int i = 0; i < 10000; i++) {
            long delay = repository.retryDelayMillis(1);
            assertTrue(delay >= 500L && delay <= 1000L, "delay=" + delay);
        }
    }

    @Test
    void jitterPercentageCalculationDoesNotOverflowAtLongMaximum() {
        JdbcSagaTccRepository repository = repository(Long.MAX_VALUE, Long.MAX_VALUE, 50);
        long expectedLowerBound = Long.MAX_VALUE - (Long.MAX_VALUE / 2L);

        for (int i = 0; i < 10000; i++) {
            long delay = repository.retryDelayMillis(0);
            assertTrue(delay >= expectedLowerBound, "overflow produced an unsafe near-zero delay: " + delay);
            assertTrue(delay <= Long.MAX_VALUE);
        }
    }

    private static JdbcSagaTccRepository repository(long baseDelay, long maximumDelay, int jitterPercent) {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setRetryBaseDelayMillis(baseDelay);
        properties.setRetryMaxDelayMillis(maximumDelay);
        properties.setRetryJitterPercent(jitterPercent);
        return new JdbcSagaTccRepository(mock(JdbcTemplate.class), properties);
    }
}
