package com.sagatcc.spring.messaging;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;
import com.sagatcc.spring.store.JdbcSagaTccRepository;

import org.springframework.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboxPublisherJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisherJob.class);

    private final SagaTccCoordinator coordinator;
    private final SagaTccProperties properties;
    private final Executor executor;
    private final AtomicInteger activePublishers = new AtomicInteger();
    private final AtomicBoolean recoveryRunning = new AtomicBoolean();

    public OutboxPublisherJob(SagaTccCoordinator coordinator, SagaTccProperties properties) {
        this(coordinator, properties, Runnable::run);
    }

    public OutboxPublisherJob(SagaTccCoordinator coordinator, SagaTccProperties properties, Executor executor) {
        this.coordinator = coordinator;
        this.properties = properties;
        this.executor = executor;
    }

    /** @deprecated repository 和 publisher 由协调器持有。 */
    @Deprecated
    public OutboxPublisherJob(JdbcSagaTccRepository repository, SagaMessagePublisher publisher,
                              SagaTccCoordinator coordinator, SagaTccProperties properties) {
        this(coordinator, properties);
    }

    @Scheduled(fixedDelayString = "${sagatcc.outbox-scan-delay:1000}")
    public void publishPending() {
        if (!properties.isSchedulerEnabled()) {
            return;
        }
        int availableSlots = properties.getOutboxPublishConcurrency() - activePublishers.get();
        for (int i = 0; i < availableSlots; i++) {
            submitPublisher();
        }
    }

    @Scheduled(fixedDelayString = "${sagatcc.recovery-scan-delay:3000}")
    public void recoverBranches() {
        if (!properties.isSchedulerEnabled() || !recoveryRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    coordinator.recoverTimeoutBranches();
                } catch (RuntimeException e) {
                    LOGGER.error("SagaTcc branch recovery failed", e);
                } finally {
                    recoveryRunning.set(false);
                }
            });
        } catch (RejectedExecutionException e) {
            recoveryRunning.set(false);
            LOGGER.warn("SagaTcc recovery task was rejected", e);
        }
    }

    private void submitPublisher() {
        int active;
        do {
            active = activePublishers.get();
            if (active >= properties.getOutboxPublishConcurrency()) {
                return;
            }
        } while (!activePublishers.compareAndSet(active, active + 1));
        try {
            executor.execute(() -> {
                try {
                    coordinator.publishPendingOutbox();
                } catch (RuntimeException e) {
                    LOGGER.error("SagaTcc outbox publishing failed", e);
                } finally {
                    activePublishers.decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            activePublishers.decrementAndGet();
            LOGGER.warn("SagaTcc outbox task was rejected", e);
        }
    }
}
