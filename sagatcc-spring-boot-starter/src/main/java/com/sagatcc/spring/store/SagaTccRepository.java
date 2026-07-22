package com.sagatcc.spring.store;

import java.util.ArrayList;
import java.util.List;

import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;

/**
 * 协调器状态和事务 outbox 的持久化扩展点。
 * 自定义存储必须保留状态迁移和抢占方法的比较并设置语义。
 */
public interface SagaTccRepository {

    void insertTransaction(SagaTccContext context, int branchCount);

    long insertBranch(String sagaId, int branchNo, String targetApp, String busCode,
                      String requestClass, String requestJson);

    /** @deprecated 安全的阶段隔离必须包含 action 和 attempt 元数据。 */
    @Deprecated
    void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag, String payload);

    default void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag,
                               SagaTccAction action, int commandAttempt, String payload) {
        throw new UnsupportedOperationException("custom SagaTccRepository must persist outbox action and attempt");
    }

    SagaTccTransactionRecord findTransaction(String sagaId);

    /**
     * 锁定事务行，使同一 Saga 的结果串行处理。
     * 默认实现直接快速失败，防止自定义存储在缺少必要并发保证时静默运行。
     */
    default SagaTccTransactionRecord findTransactionForUpdate(String sagaId) {
        throw new UnsupportedOperationException("custom SagaTccRepository must lock the parent transaction row");
    }

    SagaTccBranchRecord findBranch(long branchId);

    /** 仅在工作线程通过重试 CAS 竞争后加载序列化请求。 */
    default SagaTccBranchRecord findBranchWithPayload(long branchId) {
        return findBranch(branchId);
    }

    /**
     * 按 {@code branchNo} 升序返回 Saga 的全部分支。
     * 顺序调度依赖该稳定顺序，Cancel 阶段会在协调器中逆序遍历结果。
     */
    List<SagaTccBranchRecord> findBranches(String sagaId);

    boolean allBranchesInStatus(String sagaId, SagaTccBranchStatus status);

    void updateTransactionStatus(String sagaId, SagaTccTransactionStatus status);

    boolean transitionTransactionStatus(String sagaId, SagaTccTransactionStatus status,
                                        SagaTccTransactionStatus... expectedStatuses);

    /**
     * 仅当所有分支都进入终态后，才结束 Confirm/Cancel 阶段。
     * 任一分支失败时阶段进入 {@code FAILED}，否则进入
     * {@code completedTransactionStatus}。
     */
    default boolean completeTransactionPhase(String sagaId,
                                             SagaTccTransactionStatus expectedTransactionStatus,
                                             SagaTccTransactionStatus completedTransactionStatus,
                                             SagaTccBranchStatus completedBranchStatus) {
        List<SagaTccBranchRecord> branches = findBranches(sagaId);
        if (branches.isEmpty()) {
            return false;
        }
        boolean failed = false;
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.FAILED) {
                failed = true;
            } else if (branch.getStatus() != completedBranchStatus) {
                return false;
            }
        }
        return transitionTransactionStatus(sagaId,
                failed ? SagaTccTransactionStatus.FAILED : completedTransactionStatus,
                expectedTransactionStatus);
    }

    void updateBranchStatus(long branchId, SagaTccBranchStatus status, String error);

    boolean transitionBranchStatus(long branchId, SagaTccBranchStatus expectedStatus,
                                   SagaTccBranchStatus status, String error);

    default boolean recordBranchFailure(long branchId, SagaTccBranchStatus expectedStatus,
                                        int attempt, String error) {
        throw new UnsupportedOperationException("custom SagaTccRepository must deduplicate failure attempts");
    }

    void markActionDispatched(long branchId, SagaTccBranchStatus status, int attempt);

    boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                int currentAttempt, int nextAttempt);

    default boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                        int currentAttempt, int nextAttempt,
                                        SagaTccTransactionStatus expectedTransactionStatus) {
        throw new UnsupportedOperationException("custom SagaTccRepository must fence retries by parent phase");
    }

    SagaTccOutboxRecord claimNextReadyOutbox();

    /**
     * 最多抢占 {@code limit} 行。实现类应重写为基于集合的批量抢占；
     * 默认实现用于保持自定义存储的源码兼容性。
     */
    default List<SagaTccOutboxRecord> claimReadyOutbox(int limit) {
        List<SagaTccOutboxRecord> claimed = new ArrayList<SagaTccOutboxRecord>();
        for (int i = 0; i < limit; i++) {
            SagaTccOutboxRecord record = claimNextReadyOutbox();
            if (record == null) {
                break;
            }
            claimed.add(record);
        }
        return claimed;
    }

    void markOutboxSent(SagaTccOutboxRecord outbox);

    void markOutboxFailed(SagaTccOutboxRecord outbox, int attempts);

    List<SagaTccBranchRecord> findRetryableBranches();

    void scheduleBranchRetry(long branchId, int attempts);
}
