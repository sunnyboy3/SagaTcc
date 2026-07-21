package com.sagatcc.spring.job;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;
import com.sagatcc.spring.messaging.OutboxPublisherJob;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class OutboxPublisherJobTest {

    @Test
    void disabledSchedulerDoesNotSubmitPublisherOrRecoveryWork() {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        SagaTccProperties properties = properties(4);
        properties.setSchedulerEnabled(false);
        AtomicInteger submissions = new AtomicInteger();
        Executor executor = command -> submissions.incrementAndGet();
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties, executor);

        job.publishPending();
        job.recoverBranches();

        assertEquals(0, submissions.get());
        verifyNoInteractions(coordinator);
    }

    @Test
    void concurrentSchedulerTriggersNeverExceedConfiguredPublisherLimit() throws Exception {
        int concurrency = 4;
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        SagaTccProperties properties = properties(concurrency);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maximumRunning = new AtomicInteger();
        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch publishersStarted = new CountDownLatch(concurrency);
        CountDownLatch releasePublishers = new CountDownLatch(1);
        CountDownLatch publishersCompleted = new CountDownLatch(concurrency);

        doAnswer(invocation -> {
            invocations.incrementAndGet();
            int active = running.incrementAndGet();
            maximumRunning.updateAndGet(previous -> Math.max(previous, active));
            publishersStarted.countDown();
            try {
                assertTrue(releasePublishers.await(5, TimeUnit.SECONDS));
            } finally {
                running.decrementAndGet();
                publishersCompleted.countDown();
            }
            return null;
        }).when(coordinator).publishPendingOutbox();

        ExecutorService publisherPool = Executors.newFixedThreadPool(8);
        ExecutorService triggerPool = Executors.newFixedThreadPool(16);
        CountDownLatch startTriggers = new CountDownLatch(1);
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties, publisherPool);
        List<Future<?>> triggers = new ArrayList<Future<?>>();
        try {
            for (int i = 0; i < 64; i++) {
                triggers.add(triggerPool.submit(() -> {
                    assertTrue(startTriggers.await(5, TimeUnit.SECONDS));
                    job.publishPending();
                    return null;
                }));
            }
            startTriggers.countDown();
            for (Future<?> trigger : triggers) {
                trigger.get(5, TimeUnit.SECONDS);
            }

            assertTrue(publishersStarted.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 20; i++) {
                job.publishPending();
            }
            assertEquals(concurrency, invocations.get(), "full publisher slots must suppress re-entry");
            assertEquals(concurrency, maximumRunning.get());
        } finally {
            releasePublishers.countDown();
            assertTrue(publishersCompleted.await(5, TimeUnit.SECONDS));
            triggerPool.shutdownNow();
            publisherPool.shutdownNow();
        }
    }

    @Test
    void recoveryTaskIsSingleFlightAndCanRunAgainAfterCompletion() throws Exception {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        SagaTccProperties properties = properties(2);
        AtomicInteger recoveries = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch tasksCompleted = new CountDownLatch(2);

        doAnswer(invocation -> {
            if (recoveries.incrementAndGet() == 1) {
                firstStarted.countDown();
                assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
            }
            return null;
        }).when(coordinator).recoverTimeoutBranches();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Executor trackingExecutor = command -> pool.execute(() -> {
            try {
                command.run();
            } finally {
                tasksCompleted.countDown();
            }
        });
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties, trackingExecutor);
        try {
            job.recoverBranches();
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            job.recoverBranches();
            job.recoverBranches();
            assertEquals(1, recoveries.get(), "overlapping recovery scans must be suppressed");

            releaseFirst.countDown();
            assertTrue(awaitCount(tasksCompleted, 1, 5, TimeUnit.SECONDS));
            job.recoverBranches();
            assertTrue(tasksCompleted.await(5, TimeUnit.SECONDS));
            assertEquals(2, recoveries.get());
        } finally {
            releaseFirst.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void publisherRuntimeFailureReleasesSlotForNextTrigger() {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        AtomicInteger invocations = new AtomicInteger();
        doAnswer(invocation -> {
            invocations.incrementAndGet();
            throw new IllegalStateException("simulated publish failure");
        }).when(coordinator).publishPendingOutbox();
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties(1), Runnable::run);

        job.publishPending();
        job.publishPending();

        assertEquals(2, invocations.get());
    }

    @Test
    void recoveryRuntimeFailureClearsSingleFlightGuard() {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        AtomicInteger invocations = new AtomicInteger();
        doAnswer(invocation -> {
            invocations.incrementAndGet();
            throw new IllegalStateException("simulated recovery failure");
        }).when(coordinator).recoverTimeoutBranches();
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties(1), Runnable::run);

        job.recoverBranches();
        job.recoverBranches();

        assertEquals(2, invocations.get());
    }

    @Test
    void publisherCanRecoverAfterExecutorRejectsFirstSubmission() {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        AtomicInteger invocations = new AtomicInteger();
        doAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        }).when(coordinator).publishPendingOutbox();
        RejectFirstExecutor executor = new RejectFirstExecutor();
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties(1), executor);

        job.publishPending();
        job.publishPending();

        assertEquals(2, executor.getSubmissions());
        assertEquals(1, invocations.get());
    }

    @Test
    void recoveryCanRecoverAfterExecutorRejectsFirstSubmission() {
        SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);
        AtomicInteger invocations = new AtomicInteger();
        doAnswer(invocation -> {
            invocations.incrementAndGet();
            return null;
        }).when(coordinator).recoverTimeoutBranches();
        RejectFirstExecutor executor = new RejectFirstExecutor();
        OutboxPublisherJob job = new OutboxPublisherJob(coordinator, properties(1), executor);

        job.recoverBranches();
        job.recoverBranches();

        assertEquals(2, executor.getSubmissions());
        assertEquals(1, invocations.get());
    }

    private static SagaTccProperties properties(int publisherConcurrency) {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setOutboxPublishConcurrency(publisherConcurrency);
        return properties;
    }

    private static boolean awaitCount(CountDownLatch latch, long expectedCount,
                                      long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (latch.getCount() != expectedCount && System.nanoTime() < deadline) {
            Thread.yield();
        }
        return latch.getCount() == expectedCount;
    }

    private static final class RejectFirstExecutor implements Executor {

        private final AtomicInteger submissions = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            if (submissions.incrementAndGet() == 1) {
                throw new RejectedExecutionException("simulated rejection");
            }
            command.run();
        }

        private int getSubmissions() {
            return submissions.get();
        }
    }
}
