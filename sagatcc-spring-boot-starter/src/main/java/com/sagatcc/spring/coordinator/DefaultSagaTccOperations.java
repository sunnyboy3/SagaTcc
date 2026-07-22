package com.sagatcc.spring.coordinator;

import java.util.UUID;

import javax.sql.DataSource;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccOperations;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccEnlistment;
import com.sagatcc.core.context.SagaTccContextHolder;
import com.sagatcc.spring.config.SagaTccNameResolver;
import com.sagatcc.spring.config.SagaTccProperties;

import org.springframework.core.env.Environment;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class DefaultSagaTccOperations implements SagaTccOperations {

    private final SagaTccCoordinator coordinator;
    private final String applicationName;
    private final int maxBranchesPerSaga;
    private final DataSource dataSource;

    public DefaultSagaTccOperations(SagaTccCoordinator coordinator, SagaTccProperties properties, Environment environment) {
        this(coordinator, properties, environment, null);
    }

    public DefaultSagaTccOperations(SagaTccCoordinator coordinator, SagaTccProperties properties,
                                    Environment environment, DataSource dataSource) {
        this.coordinator = coordinator;
        this.applicationName = SagaTccNameResolver.applicationName(properties, environment);
        this.maxBranchesPerSaga = properties.getMaxBranchesPerSaga();
        this.dataSource = dataSource;
    }

    @Override
    public String begin(String businessCode, String businessId) {
        return begin(businessCode, businessId, UUID.randomUUID().toString().replace("-", ""));
    }

    @Override
    public String begin(String businessCode, String businessId, String sagaId) {
        if (SagaTccContextHolder.get() != null) {
            throw new SagaTccException("SagaTcc transaction already started in current thread");
        }
        requireActiveWritableTransaction();
        String normalizedBusinessCode = requireText(businessCode, "businessCode", 128);
        String normalizedBusinessId = requireText(businessId, "businessId", 128);
        String normalizedSagaId = requireText(sagaId, "sagaId", 64);
        SagaTccContext context = new SagaTccContext(normalizedSagaId, applicationName,
                normalizedBusinessCode, normalizedBusinessId);
        SagaTccContextHolder.set(context);
        try {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                private boolean suspended;

                @Override
                public void beforeCommit(boolean readOnly) {
                    coordinator.persistAndScheduleTry(context);
                    context.setPersisted(true);
                }

                @Override
                public void suspend() {
                    if (SagaTccContextHolder.get() == context) {
                        SagaTccContextHolder.clear();
                        suspended = true;
                    }
                }

                @Override
                public void resume() {
                    if (suspended) {
                        if (SagaTccContextHolder.get() != null) {
                            throw new SagaTccException("cannot restore suspended SagaTcc context over another context");
                        }
                        SagaTccContextHolder.set(context);
                        suspended = false;
                    }
                }

                @Override
                public void afterCompletion(int status) {
                    if (SagaTccContextHolder.get() == context) {
                        SagaTccContextHolder.clear();
                    }
                }
            });
        } catch (RuntimeException e) {
            SagaTccContextHolder.clear();
            throw e;
        }
        return normalizedSagaId;
    }

    @Override
    public void enlist(SagaTccRequest request) {
        if (request == null) {
            throw new SagaTccException("SagaTcc request must not be null");
        }
        SagaTccBusiness business = request.getClass().getAnnotation(SagaTccBusiness.class);
        if (business == null) {
            throw new SagaTccException("SagaTcc request missing @SagaTccBusiness: " + request.getClass().getName()
                    + "; alternatively use enlist(targetApplication, businessCode, request)");
        }
        enlist(business.appId(), business.busCode(), request);
    }

    @Override
    public void enlist(String targetApplication, String businessCode, SagaTccRequest request) {
        if (request == null) {
            throw new SagaTccException("SagaTcc request must not be null");
        }
        SagaTccContext context = SagaTccContextHolder.get();
        if (context == null) {
            throw new SagaTccException("SagaTcc transaction has not started; call begin inside an active transaction first");
        }
        if (context.isPersisted()) {
            throw new SagaTccException("SagaTcc transaction is already persisted and can no longer accept branches");
        }
        if (context.getEnlistments().size() >= maxBranchesPerSaga) {
            throw new SagaTccException("SagaTcc transaction exceeds max branches: " + maxBranchesPerSaga);
        }
        requireActiveWritableTransaction();
        String normalizedTargetApplication = requireText(targetApplication, "targetApplication", 128);
        SagaTccNameResolver.validateApplicationName(normalizedTargetApplication);
        context.addEnlistment(new SagaTccEnlistment(
                normalizedTargetApplication,
                requireText(businessCode, "businessCode", 128), request));
    }

    @Override
    public String currentSagaId() {
        SagaTccContext context = SagaTccContextHolder.get();
        return context == null ? null : context.getSagaId();
    }

    private void requireActiveWritableTransaction() {
        if (!TransactionSynchronizationManager.isSynchronizationActive()
                || !TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new SagaTccException("SagaTcc operations require an active Spring transaction");
        }
        if (TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
            throw new SagaTccException("SagaTcc operations cannot run in a read-only transaction");
        }
        try {
            if (TransactionAspectSupport.currentTransactionStatus().hasSavepoint()) {
                throw new SagaTccException("SagaTcc operations cannot run inside a NESTED/savepoint transaction; "
                        + "a savepoint rollback cannot remove transaction synchronizations safely");
            }
        } catch (NoTransactionException ignored) {
            // 编程式事务管理器不一定通过 TransactionAspectSupport 暴露状态。
            // 下方的事务活动状态和资源检查仍能保护受支持的
            // REQUIRED/REQUIRES_NEW 路径。
        }
        if (dataSource != null && !TransactionSynchronizationManager.hasResource(dataSource)) {
            throw new SagaTccException("the active transaction does not manage the SagaTcc JDBC DataSource; "
                    + "configure sagatcc.transaction-manager-bean-name to the business transaction manager");
        }
    }

    private String requireText(String value, String field, int maxLength) {
        if (value == null || value.trim().length() == 0) {
            throw new SagaTccException(field + " must not be blank");
        }
        String normalized = value.trim();
        if (normalized.length() > maxLength) {
            throw new SagaTccException(field + " must not exceed " + maxLength + " characters");
        }
        return normalized;
    }
}
