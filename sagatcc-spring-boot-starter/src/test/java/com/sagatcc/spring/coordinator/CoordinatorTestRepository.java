package com.sagatcc.spring.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.store.SagaTccRepository;

/**
 * 协调器并发测试使用的轻量级比较并设置仓库。
 * 扫描时有意返回分离的行对象，以模拟 JDBC 查询行为，
 * 避免竞争中的工作线程共享可变记录。
 */
final class CoordinatorTestRepository implements SagaTccRepository {

    private final Map<String, SagaTccTransactionRecord> transactions =
            new ConcurrentHashMap<String, SagaTccTransactionRecord>();
    private final Map<Long, SagaTccBranchRecord> branches =
            new ConcurrentHashMap<Long, SagaTccBranchRecord>();
    private final Set<Long> branchesWithPendingCommand =
            Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    private final List<SagaTccOutboxRecord> enqueuedOutbox =
            Collections.synchronizedList(new ArrayList<SagaTccOutboxRecord>());
    private final AtomicLong branchIds = new AtomicLong(100L);
    private final AtomicInteger scheduledBranchRetries = new AtomicInteger();
    private final AtomicInteger transactionForUpdateLookups = new AtomicInteger();
    private final Map<SagaTccTransactionStatus, Integer> successfulTransactionTransitions =
            new EnumMap<SagaTccTransactionStatus, Integer>(SagaTccTransactionStatus.class);
    private volatile CountDownLatch retryScanBarrier;

    void addTransaction(String sagaId, String coordinatorApp, SagaTccTransactionStatus status) {
        addTransaction(sagaId, coordinatorApp, null, status);
    }

    void addTransaction(String sagaId, String coordinatorApp, String businessCode,
                        SagaTccTransactionStatus status) {
        SagaTccTransactionRecord transaction = new SagaTccTransactionRecord();
        transaction.setSagaId(sagaId);
        transaction.setCoordinatorApp(coordinatorApp);
        transaction.setBusinessCode(businessCode);
        transaction.setStatus(status);
        transactions.put(sagaId, transaction);
    }

    void addBranch(long id, String sagaId, SagaTccBranchStatus status) {
        SagaTccBranchRecord branch = new SagaTccBranchRecord();
        branch.setId(id);
        branch.setSagaId(sagaId);
        branch.setBranchNo((int) id);
        branch.setTargetApp("wallet");
        branch.setBusCode("reserve");
        branch.setRequestClass("example.ReserveRequest");
        branch.setRequestJson("{}");
        branch.setStatus(status);
        branches.put(id, branch);
    }

    synchronized SagaTccBranchStatus branchStatus(long branchId) {
        return branches.get(branchId).getStatus();
    }

    synchronized SagaTccTransactionStatus transactionStatus(String sagaId) {
        return transactions.get(sagaId).getStatus();
    }

    synchronized int attempts(long branchId, SagaTccBranchStatus status) {
        return attempts(branches.get(branchId), status);
    }

    synchronized void setDispatchedAttempts(long branchId, int tryAttempts,
                                            int confirmAttempts, int cancelAttempts) {
        SagaTccBranchRecord branch = branches.get(branchId);
        branch.setTryAttempts(tryAttempts);
        branch.setConfirmAttempts(confirmAttempts);
        branch.setCancelAttempts(cancelAttempts);
    }

    synchronized int successfulTransitionsTo(SagaTccTransactionStatus status) {
        Integer count = successfulTransactionTransitions.get(status);
        return count == null ? 0 : count;
    }

    int scheduledBranchRetries() {
        return scheduledBranchRetries.get();
    }

    int transactionForUpdateLookups() {
        return transactionForUpdateLookups.get();
    }

    int enqueuedCount() {
        return enqueuedOutbox.size();
    }

    List<SagaTccOutboxRecord> enqueuedOutbox() {
        synchronized (enqueuedOutbox) {
            return new ArrayList<SagaTccOutboxRecord>(enqueuedOutbox);
        }
    }

    void synchronizeRetryScans(int workerCount) {
        retryScanBarrier = new CountDownLatch(workerCount);
    }

    void markCurrentCommandPending(long branchId) {
        branchesWithPendingCommand.add(branchId);
    }

    @Override
    public synchronized void insertTransaction(SagaTccContext context, int branchCount) {
        SagaTccTransactionRecord transaction = new SagaTccTransactionRecord();
        transaction.setSagaId(context.getSagaId());
        transaction.setCoordinatorApp(context.getCoordinatorApp());
        transaction.setBusinessCode(context.getBusinessCode());
        transaction.setBusinessId(context.getBusinessId());
        transaction.setBranchCount(branchCount);
        transaction.setStatus(SagaTccTransactionStatus.NEW);
        transactions.put(context.getSagaId(), transaction);
    }

    @Override
    public synchronized long insertBranch(String sagaId, int branchNo, String targetApp, String busCode,
                                          String requestClass, String requestJson) {
        long id = branchIds.incrementAndGet();
        SagaTccBranchRecord branch = new SagaTccBranchRecord();
        branch.setId(id);
        branch.setSagaId(sagaId);
        branch.setBranchNo(branchNo);
        branch.setTargetApp(targetApp);
        branch.setBusCode(busCode);
        branch.setRequestClass(requestClass);
        branch.setRequestJson(requestJson);
        branch.setStatus(SagaTccBranchStatus.NEW);
        branches.put(id, branch);
        return id;
    }

    @Override
    public synchronized void enqueueOutbox(String messageKey, String sagaId, long branchId,
                                           String topic, String tag, String payload) {
        enqueueOutbox(messageKey, sagaId, branchId, topic, tag, null, 0, payload);
    }

    @Override
    public synchronized void enqueueOutbox(String messageKey, String sagaId, long branchId,
                                           String topic, String tag, SagaTccAction action,
                                           int commandAttempt, String payload) {
        SagaTccOutboxRecord outbox = new SagaTccOutboxRecord();
        outbox.setId((long) enqueuedOutbox.size() + 1L);
        outbox.setMessageKey(messageKey);
        outbox.setSagaId(sagaId);
        outbox.setBranchId(branchId);
        outbox.setTopic(topic);
        outbox.setTag(tag);
        outbox.setAction(action);
        outbox.setCommandAttempt(commandAttempt);
        outbox.setPayload(payload);
        enqueuedOutbox.add(outbox);
    }

    @Override
    public synchronized SagaTccTransactionRecord findTransaction(String sagaId) {
        return copy(transactions.get(sagaId));
    }

    @Override
    public SagaTccTransactionRecord findTransactionForUpdate(String sagaId) {
        transactionForUpdateLookups.incrementAndGet();
        return findTransaction(sagaId);
    }

    @Override
    public synchronized SagaTccBranchRecord findBranch(long branchId) {
        return copy(branches.get(branchId));
    }

    @Override
    public synchronized List<SagaTccBranchRecord> findBranches(String sagaId) {
        List<SagaTccBranchRecord> result = new ArrayList<SagaTccBranchRecord>();
        for (SagaTccBranchRecord branch : branches.values()) {
            if (sagaId.equals(branch.getSagaId())) {
                result.add(copy(branch));
            }
        }
        Collections.sort(result, new Comparator<SagaTccBranchRecord>() {
            @Override
            public int compare(SagaTccBranchRecord left, SagaTccBranchRecord right) {
                int branchOrder = Integer.compare(left.getBranchNo(), right.getBranchNo());
                return branchOrder != 0 ? branchOrder : left.getId().compareTo(right.getId());
            }
        });
        return result;
    }

    @Override
    public synchronized boolean allBranchesInStatus(String sagaId, SagaTccBranchStatus status) {
        boolean found = false;
        for (SagaTccBranchRecord branch : branches.values()) {
            if (sagaId.equals(branch.getSagaId())) {
                found = true;
                if (branch.getStatus() != status) {
                    return false;
                }
            }
        }
        return found;
    }

    @Override
    public synchronized void updateTransactionStatus(String sagaId, SagaTccTransactionStatus status) {
        transactions.get(sagaId).setStatus(status);
    }

    @Override
    public synchronized boolean transitionTransactionStatus(String sagaId, SagaTccTransactionStatus status,
                                                            SagaTccTransactionStatus... expectedStatuses) {
        SagaTccTransactionRecord transaction = transactions.get(sagaId);
        if (transaction == null) {
            return false;
        }
        for (SagaTccTransactionStatus expected : expectedStatuses) {
            if (transaction.getStatus() == expected) {
                transaction.setStatus(status);
                Integer count = successfulTransactionTransitions.get(status);
                successfulTransactionTransitions.put(status, count == null ? 1 : count + 1);
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void updateBranchStatus(long branchId, SagaTccBranchStatus status, String error) {
        SagaTccBranchRecord branch = branches.get(branchId);
        branch.setStatus(status);
        branch.setLastError(error);
    }

    @Override
    public synchronized boolean transitionBranchStatus(long branchId, SagaTccBranchStatus expectedStatus,
                                                       SagaTccBranchStatus status, String error) {
        SagaTccBranchRecord branch = branches.get(branchId);
        if (branch == null || branch.getStatus() != expectedStatus) {
            return false;
        }
        branch.setStatus(status);
        branch.setLastError(error);
        return true;
    }

    @Override
    public synchronized boolean recordBranchFailure(long branchId, SagaTccBranchStatus expectedStatus,
                                                    int attempt, String error) {
        SagaTccBranchRecord branch = branches.get(branchId);
        if (branch == null || branch.getStatus() != expectedStatus || branch.getFailureAttempt() >= attempt) {
            return false;
        }
        branch.setFailureAttempt(attempt);
        branch.setLastError(error);
        return true;
    }

    @Override
    public synchronized void markActionDispatched(long branchId, SagaTccBranchStatus status, int attempt) {
        SagaTccBranchRecord branch = branches.get(branchId);
        branch.setStatus(status);
        branch.setFailureAttempt(0);
        setAttempts(branch, status, Math.max(attempts(branch, status), attempt));
    }

    @Override
    public synchronized boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                                    int currentAttempt, int nextAttempt) {
        SagaTccBranchRecord branch = branches.get(branchId);
        if (branch == null || branch.getStatus() != status || attempts(branch, status) != currentAttempt) {
            return false;
        }
        setAttempts(branch, status, nextAttempt);
        return true;
    }

    @Override
    public synchronized boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                                    int currentAttempt, int nextAttempt,
                                                    SagaTccTransactionStatus expectedTransactionStatus) {
        SagaTccBranchRecord branch = branches.get(branchId);
        SagaTccTransactionRecord transaction = branch == null ? null : transactions.get(branch.getSagaId());
        if (transaction == null || transaction.getStatus() != expectedTransactionStatus) {
            return false;
        }
        return markRetryDispatched(branchId, status, currentAttempt, nextAttempt);
    }

    @Override
    public SagaTccOutboxRecord claimNextReadyOutbox() {
        return null;
    }

    @Override
    public List<SagaTccOutboxRecord> claimReadyOutbox(int limit) {
        return Collections.emptyList();
    }

    @Override
    public void markOutboxSent(SagaTccOutboxRecord outbox) {
        throw new UnsupportedOperationException("not used by state-machine tests");
    }

    @Override
    public void markOutboxFailed(SagaTccOutboxRecord outbox, int attempts) {
        throw new UnsupportedOperationException("not used by state-machine tests");
    }

    @Override
    public List<SagaTccBranchRecord> findRetryableBranches() {
        List<SagaTccBranchRecord> result = new ArrayList<SagaTccBranchRecord>();
        synchronized (this) {
            for (SagaTccBranchRecord branch : branches.values()) {
                if (!branchesWithPendingCommand.contains(branch.getId())
                        && (branch.getStatus() == SagaTccBranchStatus.TRYING
                        || branch.getStatus() == SagaTccBranchStatus.CONFIRMING
                        || branch.getStatus() == SagaTccBranchStatus.CANCELLING)) {
                    result.add(copy(branch));
                }
            }
        }
        CountDownLatch barrier = retryScanBarrier;
        if (barrier != null) {
            barrier.countDown();
            try {
                if (!barrier.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("retry workers did not reach the scan barrier");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("retry scan interrupted", e);
            }
        }
        return result;
    }

    @Override
    public void scheduleBranchRetry(long branchId, int attempts) {
        // JDBC 调度只更新时间戳；attempt 在实际派发时递增。
        scheduledBranchRetries.incrementAndGet();
    }

    private int attempts(SagaTccBranchRecord branch, SagaTccBranchStatus status) {
        if (status == SagaTccBranchStatus.TRYING) {
            return branch.getTryAttempts();
        }
        if (status == SagaTccBranchStatus.CONFIRMING) {
            return branch.getConfirmAttempts();
        }
        if (status == SagaTccBranchStatus.CANCELLING) {
            return branch.getCancelAttempts();
        }
        throw new IllegalArgumentException("not dispatchable: " + status);
    }

    private void setAttempts(SagaTccBranchRecord branch, SagaTccBranchStatus status, int attempts) {
        if (status == SagaTccBranchStatus.TRYING) {
            branch.setTryAttempts(attempts);
        } else if (status == SagaTccBranchStatus.CONFIRMING) {
            branch.setConfirmAttempts(attempts);
        } else if (status == SagaTccBranchStatus.CANCELLING) {
            branch.setCancelAttempts(attempts);
        } else {
            throw new IllegalArgumentException("not dispatchable: " + status);
        }
    }

    private SagaTccTransactionRecord copy(SagaTccTransactionRecord source) {
        if (source == null) {
            return null;
        }
        SagaTccTransactionRecord copy = new SagaTccTransactionRecord();
        copy.setSagaId(source.getSagaId());
        copy.setCoordinatorApp(source.getCoordinatorApp());
        copy.setBusinessCode(source.getBusinessCode());
        copy.setBusinessId(source.getBusinessId());
        copy.setStatus(source.getStatus());
        copy.setBranchCount(source.getBranchCount());
        copy.setLastError(source.getLastError());
        return copy;
    }

    private SagaTccBranchRecord copy(SagaTccBranchRecord source) {
        if (source == null) {
            return null;
        }
        SagaTccBranchRecord copy = new SagaTccBranchRecord();
        copy.setId(source.getId());
        copy.setSagaId(source.getSagaId());
        copy.setBranchNo(source.getBranchNo());
        copy.setTargetApp(source.getTargetApp());
        copy.setBusCode(source.getBusCode());
        copy.setRequestClass(source.getRequestClass());
        copy.setRequestJson(source.getRequestJson());
        copy.setStatus(source.getStatus());
        copy.setTryAttempts(source.getTryAttempts());
        copy.setConfirmAttempts(source.getConfirmAttempts());
        copy.setCancelAttempts(source.getCancelAttempts());
        copy.setFailureAttempt(source.getFailureAttempt());
        copy.setLastError(source.getLastError());
        return copy;
    }
}
