package com.sagatcc.spring.coordinator;

import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.sql.DataSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccEnlistment;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.core.model.SagaTccBranchRecord;
import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.core.model.SagaTccOutboxRecord;
import com.sagatcc.core.model.SagaTccTransactionRecord;
import com.sagatcc.core.model.SagaTccTransactionStatus;
import com.sagatcc.spring.config.SagaTccBranchExecutionMode;
import com.sagatcc.spring.config.SagaTccNameResolver;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.store.JdbcSagaTccRepository;
import com.sagatcc.spring.store.SagaTccRepository;

import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SagaTccCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SagaTccCoordinator.class);

    private final SagaTccRepository repository;
    private final SagaMessagePublisher publisher;
    private final ObjectMapper objectMapper;
    private final SagaTccProperties properties;
    private final String localApplication;
    private final TransactionTemplate recoveryTransaction;
    private final DataSource dataSource;

    public SagaTccCoordinator(SagaTccRepository repository, SagaMessagePublisher publisher,
                              ObjectMapper objectMapper, SagaTccProperties properties) {
        this.repository = repository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.localApplication = null;
        this.recoveryTransaction = null;
        this.dataSource = null;
    }

    public SagaTccCoordinator(SagaTccRepository repository, SagaMessagePublisher publisher,
                              ObjectMapper objectMapper, SagaTccProperties properties, String localApplication) {
        this.repository = repository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.localApplication = localApplication;
        this.recoveryTransaction = null;
        this.dataSource = null;
    }

    public SagaTccCoordinator(SagaTccRepository repository, SagaMessagePublisher publisher,
                              ObjectMapper objectMapper, SagaTccProperties properties, String localApplication,
                              TransactionTemplate recoveryTransaction) {
        this.repository = repository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.localApplication = localApplication;
        this.recoveryTransaction = recoveryTransaction;
        this.dataSource = null;
    }

    public SagaTccCoordinator(SagaTccRepository repository, SagaMessagePublisher publisher,
                              ObjectMapper objectMapper, SagaTccProperties properties, String localApplication,
                              TransactionTemplate recoveryTransaction, DataSource dataSource) {
        this.repository = repository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.localApplication = localApplication;
        this.recoveryTransaction = recoveryTransaction;
        this.dataSource = dataSource;
    }

    /** @deprecated 请使用接收 {@link SagaTccRepository} 的构造方法。 */
    @Deprecated
    public SagaTccCoordinator(JdbcSagaTccRepository repository, SagaMessagePublisher publisher,
                              ObjectMapper objectMapper, SagaTccProperties properties, Environment environment) {
        this(repository, publisher, objectMapper, properties,
                SagaTccNameResolver.applicationName(properties, environment));
    }

    @Transactional(transactionManager = "sagaTccTransactionManager")
    public void persistAndScheduleTry(SagaTccContext context) {
        requireManagedDataSource();
        if (context.isPersisted()) {
            return;
        }
        repository.insertTransaction(context, context.getEnlistments().size());
        if (context.getEnlistments().isEmpty()) {
            repository.updateTransactionStatus(context.getSagaId(), SagaTccTransactionStatus.COMMITTED);
            return;
        }
        int branchNo = 1;
        long totalPayloadBytes = 0L;
        for (SagaTccEnlistment enlistment : context.getEnlistments()) {
            SagaTccRequest request = enlistment.getRequest();
            String requestJson = toJson(request);
            int requestBytes = requestJson.getBytes(StandardCharsets.UTF_8).length;
            if (requestBytes > properties.getMaxRequestBytes()) {
                throw new SagaTccException("serialized SagaTcc request exceeds max-request-bytes: "
                        + request.getClass().getName());
            }
            totalPayloadBytes += requestBytes;
            if (totalPayloadBytes > properties.getMaxSagaPayloadBytes()) {
                throw new SagaTccException("serialized SagaTcc requests exceed max-saga-payload-bytes");
            }
            int currentBranchNo = branchNo++;
            long branchId = repository.insertBranch(context.getSagaId(), currentBranchNo,
                    enlistment.getTargetApplication(),
                    enlistment.getBusinessCode(), request.getClass().getName(), requestJson);
            if (!isSequentialBranchExecution(context.getBusinessCode()) || currentBranchNo == 1) {
                enqueueCommand(context.getSagaId(), branchId, context.getCoordinatorApp(),
                        enlistment.getTargetApplication(), enlistment.getBusinessCode(), SagaTccAction.TRY,
                        request.getClass().getName(), requestJson, 1);
                repository.markActionDispatched(branchId, SagaTccBranchStatus.TRYING, 1);
            }
        }
        repository.updateTransactionStatus(context.getSagaId(), SagaTccTransactionStatus.TRYING);
    }

    @Transactional(transactionManager = "sagaTccTransactionManager")
    public void handleResult(SagaTccResultMessage result) {
        requireManagedDataSource();
        if (result == null || result.getSagaId() == null || result.getBranchId() == null || result.getAction() == null
                || !result.successSpecified() || !result.retryableSpecified()
                || (localApplication != null && !equals(localApplication, result.getCoordinatorApp()))) {
            return;
        }
        SagaTccTransactionRecord transaction = repository.findTransactionForUpdate(result.getSagaId());
        if (transaction == null || isFinal(transaction.getStatus())
                || !equals(transaction.getCoordinatorApp(), result.getCoordinatorApp())) {
            return;
        }
        SagaTccBranchRecord branch = repository.findBranch(result.getBranchId());
        if (branch == null || !matches(result, branch) || !isAcceptableAttempt(result, branch)) {
            return;
        }
        if (result.getAction() == SagaTccAction.TRY) {
            handleTryResult(result, branch, transaction);
        } else if (result.getAction() == SagaTccAction.CONFIRM) {
            handleConfirmResult(result, branch, transaction);
        } else if (result.getAction() == SagaTccAction.CANCEL) {
            handleCancelResult(result, branch, transaction);
        }
    }

    public void publishPendingOutbox() {
        int remaining = properties.getScanBatchSize();
        while (remaining > 0) {
            int claimSize = Math.min(remaining, properties.getOutboxClaimBatchSize());
            List<SagaTccOutboxRecord> records = repository.claimReadyOutbox(claimSize);
            if (records.isEmpty()) {
                return;
            }
            for (SagaTccOutboxRecord record : records) {
                try {
                    publisher.publishRaw(record.getTopic(), record.getTag(), record.getMessageKey(), record.getPayload());
                    repository.markOutboxSent(record);
                } catch (Exception e) {
                    repository.markOutboxFailed(record, record.getAttempts() + 1);
                }
            }
            remaining -= records.size();
            if (records.size() < claimSize) {
                return;
            }
        }
    }

    public void recoverTimeoutBranches() {
        List<SagaTccBranchRecord> branches = repository.findRetryableBranches();
        for (SagaTccBranchRecord branch : branches) {
            try {
                if (recoveryTransaction == null) {
                    recoverBranch(branch);
                } else {
                    recoveryTransaction.execute(status -> {
                        recoverBranch(branch);
                        return null;
                    });
                }
            } catch (RuntimeException e) {
                LOGGER.warn("SagaTcc branch recovery failed, sagaId={}, branchId={}",
                        branch.getSagaId(), branch.getId(), e);
            }
        }
    }

    private void recoverBranch(SagaTccBranchRecord branch) {
        requireManagedDataSource();
        SagaTccTransactionRecord tx = repository.findTransactionForUpdate(branch.getSagaId());
        if (tx == null || isFinal(tx.getStatus())) {
            return;
        }
        SagaTccAction action = actionFor(branch.getStatus());
        if (action == null) {
            return;
        }
        int attempts = attemptsFor(branch, action);
        if (attempts >= properties.getMaxAttempts()) {
            handleExhaustedBranch(branch, action, tx);
            return;
        }
        int nextAttempt = attempts + 1;
        if (repository.markRetryDispatched(branch.getId(), branch.getStatus(), attempts, nextAttempt,
                transactionStatusFor(action))) {
            SagaTccBranchRecord dispatchBranch = branch.getRequestClass() != null && branch.getRequestJson() != null
                    ? branch : repository.findBranchWithPayload(branch.getId());
            if (dispatchBranch == null) {
                throw new SagaTccException("SagaTcc branch disappeared after retry claim: " + branch.getId());
            }
            enqueueCommand(dispatchBranch.getSagaId(), dispatchBranch.getId(), tx.getCoordinatorApp(),
                    dispatchBranch.getTargetApp(), dispatchBranch.getBusCode(), action,
                    dispatchBranch.getRequestClass(), dispatchBranch.getRequestJson(), nextAttempt);
        }
    }

    private void handleExhaustedBranch(SagaTccBranchRecord branch, SagaTccAction action,
                                       SagaTccTransactionRecord transaction) {
        if (action == SagaTccAction.TRY) {
            if (repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.TRYING,
                    SagaTccBranchStatus.TRY_FAILED, "max retry attempts reached")) {
                completeTryPhase(branch.getSagaId(), transaction);
            }
            return;
        }
        if (repository.transitionBranchStatus(branch.getId(), branch.getStatus(), SagaTccBranchStatus.FAILED,
                "max retry attempts reached")) {
            if (completeTerminalPhase(branch.getSagaId(), action, transaction)) {
                transaction.setStatus(SagaTccTransactionStatus.FAILED);
            }
        }
    }

    private void handleTryResult(SagaTccResultMessage result, SagaTccBranchRecord branch,
                                 SagaTccTransactionRecord transaction) {
        if (!result.isSuccess()) {
            if (result.isRetryable()) {
                if (repository.recordBranchFailure(branch.getId(), SagaTccBranchStatus.TRYING,
                        result.getAttempt(), result.getErrorMessage())) {
                    repository.scheduleBranchRetry(branch.getId(), branch.getTryAttempts() + 1);
                }
                return;
            }
            if (!repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.TRYING,
                    SagaTccBranchStatus.TRY_FAILED, result.getErrorMessage())) {
                return;
            }
            completeTryPhase(branch.getSagaId(), transaction);
            return;
        }
        if (!repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.TRYING,
                SagaTccBranchStatus.TRY_SUCCEEDED, null)) {
            return;
        }
        completeTryPhase(branch.getSagaId(), transaction);
    }

    private void handleConfirmResult(SagaTccResultMessage result, SagaTccBranchRecord branch,
                                     SagaTccTransactionRecord transaction) {
        if (result.isSuccess()) {
            if (!repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.CONFIRMING,
                    SagaTccBranchStatus.CONFIRMED, null)) {
                return;
            }
        } else {
            if (!result.isRetryable()) {
                failPermanently(branch, SagaTccBranchStatus.CONFIRMING,
                        result.getErrorMessage(), transaction);
                return;
            }
            if (!repository.recordBranchFailure(branch.getId(), SagaTccBranchStatus.CONFIRMING,
                    result.getAttempt(), result.getErrorMessage())) {
                return;
            }
            repository.scheduleBranchRetry(branch.getId(), branch.getConfirmAttempts() + 1);
            return;
        }
        completeTerminalPhase(branch.getSagaId(), SagaTccAction.CONFIRM, transaction);
    }

    private void handleCancelResult(SagaTccResultMessage result, SagaTccBranchRecord branch,
                                    SagaTccTransactionRecord transaction) {
        if (result.isSuccess()) {
            if (!repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.CANCELLING,
                    SagaTccBranchStatus.CANCELLED, null)) {
                return;
            }
        } else {
            if (!result.isRetryable()) {
                failPermanently(branch, SagaTccBranchStatus.CANCELLING,
                        result.getErrorMessage(), transaction);
                return;
            }
            if (!repository.recordBranchFailure(branch.getId(), SagaTccBranchStatus.CANCELLING,
                    result.getAttempt(), result.getErrorMessage())) {
                return;
            }
            repository.scheduleBranchRetry(branch.getId(), branch.getCancelAttempts() + 1);
            return;
        }
        completeTerminalPhase(branch.getSagaId(), SagaTccAction.CANCEL, transaction);
    }

    /**
     * 并行模式等待所有 Try 返回；顺序模式只在当前分支成功后调度下一个分支。
     */
    private void completeTryPhase(String sagaId, SagaTccTransactionRecord tx) {
        List<SagaTccBranchRecord> branches = repository.findBranches(sagaId);
        if (branches.isEmpty()) {
            return;
        }
        if (isSequentialBranchExecution(tx.getBusinessCode())) {
            completeSequentialTryPhase(tx, branches);
            return;
        }
        boolean hasFailure = false;
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.TRYING) {
                return;
            }
            if (branch.getStatus() == SagaTccBranchStatus.TRY_FAILED) {
                hasFailure = true;
            } else if (branch.getStatus() != SagaTccBranchStatus.TRY_SUCCEEDED) {
                return;
            }
        }
        if (hasFailure) {
            scheduleCancel(sagaId, branches);
        } else {
            scheduleConfirm(sagaId, branches);
        }
    }

    private void completeSequentialTryPhase(SagaTccTransactionRecord tx,
                                            List<SagaTccBranchRecord> branches) {
        String sagaId = tx.getSagaId();
        boolean hasFailure = false;
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.TRYING) {
                return;
            }
            if (branch.getStatus() == SagaTccBranchStatus.TRY_FAILED) {
                hasFailure = true;
            } else if (branch.getStatus() != SagaTccBranchStatus.NEW
                    && branch.getStatus() != SagaTccBranchStatus.TRY_SUCCEEDED) {
                return;
            }
        }
        if (hasFailure) {
            // 尚未执行 Try 的后续分支没有资源需要补偿，直接标记为已取消。
            for (SagaTccBranchRecord branch : branches) {
                if (branch.getStatus() == SagaTccBranchStatus.NEW) {
                    repository.transitionBranchStatus(branch.getId(), SagaTccBranchStatus.NEW,
                            SagaTccBranchStatus.CANCELLED, "前置 Try 分支失败，当前分支未执行");
                }
            }
            scheduleCancel(sagaId, repository.findBranches(sagaId));
            return;
        }
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.NEW) {
                if (tx.getStatus() == SagaTccTransactionStatus.TRYING) {
                    dispatchBranch(tx, branch, SagaTccAction.TRY,
                            SagaTccBranchStatus.TRYING, branch.getTryAttempts() + 1);
                }
                return;
            }
        }
        scheduleConfirm(sagaId, branches);
    }

    private void scheduleConfirm(String sagaId, List<SagaTccBranchRecord> branches) {
        if (!repository.transitionTransactionStatus(sagaId, SagaTccTransactionStatus.COMMITTING,
                SagaTccTransactionStatus.TRYING)) {
            return;
        }
        SagaTccTransactionRecord tx = repository.findTransaction(sagaId);
        if (tx == null) {
            return;
        }
        if (isSequentialBranchExecution(tx.getBusinessCode())) {
            dispatchNextSequentialConfirm(tx, branches);
            return;
        }
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.TRY_SUCCEEDED) {
                dispatchBranch(tx, branch, SagaTccAction.CONFIRM,
                        SagaTccBranchStatus.CONFIRMING, branch.getConfirmAttempts() + 1);
            }
        }
    }

    private void scheduleCancel(String sagaId, List<SagaTccBranchRecord> branches) {
        if (!repository.transitionTransactionStatus(sagaId, SagaTccTransactionStatus.CANCELLING,
                SagaTccTransactionStatus.TRYING)) {
            return;
        }
        SagaTccTransactionRecord tx = repository.findTransaction(sagaId);
        if (tx == null) {
            return;
        }
        if (isSequentialBranchExecution(tx.getBusinessCode())) {
            dispatchNextSequentialCancel(tx, branches);
            return;
        }
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.TRY_SUCCEEDED
                    || branch.getStatus() == SagaTccBranchStatus.TRY_FAILED) {
                dispatchBranch(tx, branch, SagaTccAction.CANCEL,
                        SagaTccBranchStatus.CANCELLING, branch.getCancelAttempts() + 1);
            }
        }
    }

    private void dispatchNextSequentialConfirm(SagaTccTransactionRecord tx,
                                               List<SagaTccBranchRecord> branches) {
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.CONFIRMING) {
                return;
            }
            if (branch.getStatus() == SagaTccBranchStatus.TRY_SUCCEEDED) {
                dispatchBranch(tx, branch, SagaTccAction.CONFIRM,
                        SagaTccBranchStatus.CONFIRMING, branch.getConfirmAttempts() + 1);
                return;
            }
            if (branch.getStatus() != SagaTccBranchStatus.CONFIRMED) {
                return;
            }
        }
    }

    private void dispatchNextSequentialCancel(SagaTccTransactionRecord tx,
                                              List<SagaTccBranchRecord> branches) {
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.CANCELLING) {
                return;
            }
        }
        for (int i = branches.size() - 1; i >= 0; i--) {
            SagaTccBranchRecord branch = branches.get(i);
            if (branch.getStatus() == SagaTccBranchStatus.TRY_SUCCEEDED
                    || branch.getStatus() == SagaTccBranchStatus.TRY_FAILED) {
                dispatchBranch(tx, branch, SagaTccAction.CANCEL,
                        SagaTccBranchStatus.CANCELLING, branch.getCancelAttempts() + 1);
                return;
            }
            if (branch.getStatus() != SagaTccBranchStatus.CANCELLED) {
                return;
            }
        }
    }

    private void dispatchBranch(SagaTccTransactionRecord tx, SagaTccBranchRecord branch,
                                SagaTccAction action, SagaTccBranchStatus status, int attempt) {
        enqueueCommand(branch.getSagaId(), branch.getId(), tx.getCoordinatorApp(),
                branch.getTargetApp(), branch.getBusCode(), action,
                branch.getRequestClass(), branch.getRequestJson(), attempt);
        repository.markActionDispatched(branch.getId(), status, attempt);
    }

    private void enqueueCommand(String sagaId, long branchId, String coordinatorApp, String targetApp, String busCode,
                                SagaTccAction action, String requestClass, String requestJson, int attempt) {
        SagaTccCommandMessage message = new SagaTccCommandMessage();
        message.setSagaId(sagaId);
        message.setBranchId(branchId);
        message.setCoordinatorApp(coordinatorApp);
        message.setTargetApp(targetApp);
        message.setBusCode(busCode);
        message.setAction(action);
        message.setRequestClass(requestClass);
        message.setRequestJson(requestJson);
        message.setAttempt(attempt);
        message.setMessageKey(sagaId + "-" + branchId + "-" + action.name() + "-" + attempt);
        String payload = toJson(message);
        if (payload.getBytes(StandardCharsets.UTF_8).length > properties.getMaxMessageBytes()) {
            throw new SagaTccException("serialized SagaTcc command exceeds max-message-bytes");
        }
        repository.enqueueOutbox(message.getMessageKey(), sagaId, branchId, publisher.commandTopic(targetApp),
                SagaMessagePublisher.COMMAND_TAG, action, attempt, payload);
    }

    private boolean matches(SagaTccResultMessage result, SagaTccBranchRecord branch) {
        return equals(branch.getSagaId(), result.getSagaId())
                && equals(branch.getTargetApp(), result.getTargetApp())
                && equals(branch.getBusCode(), result.getBusCode());
    }

    private boolean isAcceptableAttempt(SagaTccResultMessage result, SagaTccBranchRecord branch) {
        int dispatchedAttempt = attemptsFor(branch, result.getAction());
        if (result.getAttempt() <= 0 || result.getAttempt() > dispatchedAttempt) {
            return false;
        }
        return result.isSuccess() || result.getAttempt() == dispatchedAttempt;
    }

    private boolean equals(String left, String right) {
        return left == null ? right == null : left.equals(right);
    }

    private void failPermanently(SagaTccBranchRecord branch, SagaTccBranchStatus expectedStatus,
                                 String error, SagaTccTransactionRecord transaction) {
        if (repository.transitionBranchStatus(branch.getId(), expectedStatus, SagaTccBranchStatus.FAILED, error)) {
            completeTerminalPhase(branch.getSagaId(),
                    expectedStatus == SagaTccBranchStatus.CONFIRMING
                            ? SagaTccAction.CONFIRM : SagaTccAction.CANCEL, transaction);
        }
    }

    private boolean completeTerminalPhase(String sagaId, SagaTccAction action,
                                          SagaTccTransactionRecord tx) {
        if (isSequentialBranchExecution(tx.getBusinessCode())) {
            return completeSequentialTerminalPhase(tx, action);
        }
        if (action == SagaTccAction.CONFIRM) {
            return repository.completeTransactionPhase(sagaId, SagaTccTransactionStatus.COMMITTING,
                    SagaTccTransactionStatus.COMMITTED, SagaTccBranchStatus.CONFIRMED);
        }
        if (action == SagaTccAction.CANCEL) {
            return repository.completeTransactionPhase(sagaId, SagaTccTransactionStatus.CANCELLING,
                    SagaTccTransactionStatus.CANCELLED, SagaTccBranchStatus.CANCELLED);
        }
        return false;
    }

    private boolean completeSequentialTerminalPhase(SagaTccTransactionRecord tx,
                                                    SagaTccAction action) {
        String sagaId = tx.getSagaId();
        List<SagaTccBranchRecord> branches = repository.findBranches(sagaId);
        if (branches.isEmpty()) {
            return false;
        }
        if (containsFailedBranch(branches)) {
            failUndispatchedSequentialBranches(branches, action);
            return repository.completeTransactionPhase(sagaId, transactionStatusFor(action),
                    action == SagaTccAction.CONFIRM
                            ? SagaTccTransactionStatus.COMMITTED : SagaTccTransactionStatus.CANCELLED,
                    action == SagaTccAction.CONFIRM
                            ? SagaTccBranchStatus.CONFIRMED : SagaTccBranchStatus.CANCELLED);
        }
        if (action == SagaTccAction.CONFIRM) {
            dispatchNextSequentialConfirm(tx, branches);
            return repository.completeTransactionPhase(sagaId, SagaTccTransactionStatus.COMMITTING,
                    SagaTccTransactionStatus.COMMITTED, SagaTccBranchStatus.CONFIRMED);
        }
        if (action == SagaTccAction.CANCEL) {
            dispatchNextSequentialCancel(tx, branches);
            return repository.completeTransactionPhase(sagaId, SagaTccTransactionStatus.CANCELLING,
                    SagaTccTransactionStatus.CANCELLED, SagaTccBranchStatus.CANCELLED);
        }
        return false;
    }

    private boolean containsFailedBranch(List<SagaTccBranchRecord> branches) {
        for (SagaTccBranchRecord branch : branches) {
            if (branch.getStatus() == SagaTccBranchStatus.FAILED) {
                return true;
            }
        }
        return false;
    }

    private void failUndispatchedSequentialBranches(List<SagaTccBranchRecord> branches,
                                                    SagaTccAction action) {
        for (SagaTccBranchRecord branch : branches) {
            SagaTccBranchStatus status = branch.getStatus();
            boolean undispatched = action == SagaTccAction.CONFIRM
                    ? status == SagaTccBranchStatus.TRY_SUCCEEDED
                    : status == SagaTccBranchStatus.TRY_SUCCEEDED
                    || status == SagaTccBranchStatus.TRY_FAILED;
            if (undispatched) {
                repository.transitionBranchStatus(branch.getId(), status, SagaTccBranchStatus.FAILED,
                        "前置分支执行失败，顺序调度已停止");
            }
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new SagaTccException("serialize saga message failed", e);
        }
    }

    private boolean isFinal(SagaTccTransactionStatus status) {
        return status == SagaTccTransactionStatus.COMMITTED
                || status == SagaTccTransactionStatus.CANCELLED
                || status == SagaTccTransactionStatus.FAILED;
    }

    private SagaTccAction actionFor(SagaTccBranchStatus status) {
        if (status == SagaTccBranchStatus.TRYING) {
            return SagaTccAction.TRY;
        }
        if (status == SagaTccBranchStatus.CONFIRMING) {
            return SagaTccAction.CONFIRM;
        }
        if (status == SagaTccBranchStatus.CANCELLING) {
            return SagaTccAction.CANCEL;
        }
        return null;
    }

    private int attemptsFor(SagaTccBranchRecord branch, SagaTccAction action) {
        if (action == SagaTccAction.TRY) {
            return branch.getTryAttempts();
        }
        if (action == SagaTccAction.CONFIRM) {
            return branch.getConfirmAttempts();
        }
        return branch.getCancelAttempts();
    }

    private SagaTccTransactionStatus transactionStatusFor(SagaTccAction action) {
        if (action == SagaTccAction.TRY) {
            return SagaTccTransactionStatus.TRYING;
        }
        if (action == SagaTccAction.CONFIRM) {
            return SagaTccTransactionStatus.COMMITTING;
        }
        return SagaTccTransactionStatus.CANCELLING;
    }

    private boolean isSequentialBranchExecution(String businessCode) {
        return properties.resolveBranchExecutionMode(businessCode) == SagaTccBranchExecutionMode.SEQUENTIAL;
    }

    private void requireManagedDataSource() {
        if (dataSource != null && (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.hasResource(dataSource))) {
            throw new SagaTccException("SagaTcc transaction manager does not manage the configured JDBC DataSource");
        }
    }
}
