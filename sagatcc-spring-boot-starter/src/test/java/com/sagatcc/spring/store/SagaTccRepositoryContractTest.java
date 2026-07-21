package com.sagatcc.spring.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaTccRepositoryContractTest {

    @Test
    void safetyCriticalExtensionDefaultsFailFastInsteadOfWeakeningConcurrencySilently() {
        SagaTccRepository repository = new StubRepository();

        assertThatThrownBy(() -> repository.enqueueOutbox("key", "saga", 1L, "topic", "tag",
                SagaTccAction.TRY, 1, "{}"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("action and attempt");
        assertThatThrownBy(() -> repository.findTransactionForUpdate("saga"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("lock the parent");
        assertThatThrownBy(() -> repository.recordBranchFailure(
                1L, SagaTccBranchStatus.TRYING, 1, "failed"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("deduplicate failure");
        assertThatThrownBy(() -> repository.markRetryDispatched(1L, SagaTccBranchStatus.TRYING,
                1, 2, SagaTccTransactionStatus.TRYING))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("parent phase");
    }

    @Test
    void fallbackBatchClaimHandlesNonPositiveLimitsNullTailAndExactLimit() {
        StubRepository repository = new StubRepository();
        SagaTccOutboxRecord first = outbox(1L);
        SagaTccOutboxRecord second = outbox(2L);
        SagaTccOutboxRecord third = outbox(3L);

        assertThat(repository.claimReadyOutbox(0)).isEmpty();
        assertThat(repository.claimReadyOutbox(-1)).isEmpty();

        repository.claims(first, null, third);
        assertThat(repository.claimReadyOutbox(5)).containsExactly(first);

        repository.claims(first, second, third);
        assertThat(repository.claimReadyOutbox(2)).containsExactly(first, second);
    }

    private SagaTccOutboxRecord outbox(long id) {
        SagaTccOutboxRecord record = new SagaTccOutboxRecord();
        record.setId(id);
        return record;
    }

    private static final class StubRepository implements SagaTccRepository {
        private List<SagaTccOutboxRecord> claims = Collections.emptyList();
        private int claimIndex;

        void claims(SagaTccOutboxRecord... values) {
            claims = new ArrayList<SagaTccOutboxRecord>();
            Collections.addAll(claims, values);
            claimIndex = 0;
        }

        @Override
        public void insertTransaction(SagaTccContext context, int branchCount) {
        }

        @Override
        public long insertBranch(String sagaId, int branchNo, String targetApp, String busCode,
                                 String requestClass, String requestJson) {
            return 0;
        }

        @Override
        public void enqueueOutbox(String messageKey, String sagaId, long branchId,
                                  String topic, String tag, String payload) {
        }

        @Override
        public SagaTccTransactionRecord findTransaction(String sagaId) {
            return null;
        }

        @Override
        public SagaTccBranchRecord findBranch(long branchId) {
            return null;
        }

        @Override
        public List<SagaTccBranchRecord> findBranches(String sagaId) {
            return Collections.emptyList();
        }

        @Override
        public boolean allBranchesInStatus(String sagaId, SagaTccBranchStatus status) {
            return false;
        }

        @Override
        public void updateTransactionStatus(String sagaId, SagaTccTransactionStatus status) {
        }

        @Override
        public boolean transitionTransactionStatus(String sagaId, SagaTccTransactionStatus status,
                                                   SagaTccTransactionStatus... expectedStatuses) {
            return false;
        }

        @Override
        public void updateBranchStatus(long branchId, SagaTccBranchStatus status, String error) {
        }

        @Override
        public boolean transitionBranchStatus(long branchId, SagaTccBranchStatus expectedStatus,
                                              SagaTccBranchStatus status, String error) {
            return false;
        }

        @Override
        public void markActionDispatched(long branchId, SagaTccBranchStatus status, int attempt) {
        }

        @Override
        public boolean markRetryDispatched(long branchId, SagaTccBranchStatus status,
                                           int currentAttempt, int nextAttempt) {
            return false;
        }

        @Override
        public SagaTccOutboxRecord claimNextReadyOutbox() {
            return claimIndex < claims.size() ? claims.get(claimIndex++) : null;
        }

        @Override
        public void markOutboxSent(SagaTccOutboxRecord outbox) {
        }

        @Override
        public void markOutboxFailed(SagaTccOutboxRecord outbox, int attempts) {
        }

        @Override
        public List<SagaTccBranchRecord> findRetryableBranches() {
            return Collections.emptyList();
        }

        @Override
        public void scheduleBranchRetry(long branchId, int attempts) {
        }
    }
}
