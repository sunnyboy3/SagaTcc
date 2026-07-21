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
 * Persistence extension point for coordinator state and the transactional
 * outbox. Custom stores must preserve the compare-and-set semantics of the
 * transition and claim methods.
 */
public interface SagaTccRepository {

    void insertTransaction(SagaTccContext context, int branchCount);

    long insertBranch(String sagaId, int branchNo, String targetApp, String busCode,
                      String requestClass, String requestJson);

    /** @deprecated action and attempt metadata are required for safe phase fencing. */
    @Deprecated
    void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag, String payload);

    default void enqueueOutbox(String messageKey, String sagaId, long branchId, String topic, String tag,
                               SagaTccAction action, int commandAttempt, String payload) {
        throw new UnsupportedOperationException("custom SagaTccRepository must persist outbox action and attempt");
    }

    SagaTccTransactionRecord findTransaction(String sagaId);

    /**
     * Locks the transaction row so results for the same Saga are serialized.
     * The default fails fast so custom stores cannot silently run without the
     * required concurrency guarantee.
     */
    default SagaTccTransactionRecord findTransactionForUpdate(String sagaId) {
        throw new UnsupportedOperationException("custom SagaTccRepository must lock the parent transaction row");
    }

    SagaTccBranchRecord findBranch(long branchId);

    /** Loads the serialized request only after a worker has won a retry CAS. */
    default SagaTccBranchRecord findBranchWithPayload(long branchId) {
        return findBranch(branchId);
    }

    List<SagaTccBranchRecord> findBranches(String sagaId);

    boolean allBranchesInStatus(String sagaId, SagaTccBranchStatus status);

    void updateTransactionStatus(String sagaId, SagaTccTransactionStatus status);

    boolean transitionTransactionStatus(String sagaId, SagaTccTransactionStatus status,
                                        SagaTccTransactionStatus... expectedStatuses);

    /**
     * Finalizes a Confirm/Cancel phase only after every branch is terminal.
     * A phase with any failed branch becomes {@code FAILED}; otherwise it
     * reaches {@code completedTransactionStatus}.
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
     * Claims up to {@code limit} rows. Implementations should override this
     * method with a set-based claim; the default keeps custom stores source
     * compatible.
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
