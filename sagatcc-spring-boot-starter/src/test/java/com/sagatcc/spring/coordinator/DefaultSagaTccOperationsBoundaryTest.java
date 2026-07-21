package com.sagatcc.spring.coordinator;

import javax.sql.DataSource;

import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccContextHolder;
import com.sagatcc.spring.config.SagaTccProperties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.MatchAlwaysTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DefaultSagaTccOperationsBoundaryTest {

    private SagaTccCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = mock(SagaTccCoordinator.class);
    }

    @AfterEach
    void clearTransactionState() {
        SagaTccContextHolder.clear();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
    }

    @Test
    void acceptsExactlyTheConfiguredBranchLimitAndRejectsTheNextBranch() {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        activateTransaction();
        operations.begin("create-order", "student-1", "saga-limit");

        operations.enlist("wallet", "reserve", new PlainRequest("first"));
        operations.enlist("course", "reserve", new PlainRequest("second"));

        SagaTccException error = assertThrows(SagaTccException.class,
                () -> operations.enlist("coupon", "reserve", new PlainRequest("overflow")));
        assertTrue(error.getMessage().contains("max branches: 2"));
        assertEquals(2, SagaTccContextHolder.get().getEnlistments().size());
    }

    @ParameterizedTest
    @ValueSource(ints = {
            TransactionSynchronization.STATUS_COMMITTED,
            TransactionSynchronization.STATUS_ROLLED_BACK,
            TransactionSynchronization.STATUS_UNKNOWN
    })
    void transactionCompletionAlwaysRemovesTheThreadLocalContext(int completionStatus) {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        activateTransaction();
        operations.begin("create-order", "student-1", "saga-cleanup");
        assertEquals("saga-cleanup", operations.currentSagaId());

        synchronization().afterCompletion(completionStatus);

        assertNull(SagaTccContextHolder.get());
        assertNull(operations.currentSagaId());
    }

    @Test
    void rollbackCompletionDoesNotPersistOrPublishAnything() {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        activateTransaction();
        operations.begin("create-order", "student-1", "saga-rollback");
        operations.enlist("wallet", "reserve", new PlainRequest("value"));

        synchronization().afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);

        assertNull(SagaTccContextHolder.get());
        verifyNoInteractions(coordinator);
    }

    @Test
    void requiresNewSuspensionClearsAndThenRestoresTheOuterSagaContext() {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        activateTransaction();
        operations.begin("create-order", "student-1", "outer-saga");
        SagaTccContext outer = SagaTccContextHolder.get();
        TransactionSynchronization synchronization = synchronization();

        synchronization.suspend();
        assertNull(SagaTccContextHolder.get());

        synchronization.resume();
        assertEquals(outer, SagaTccContextHolder.get());
        assertEquals("outer-saga", operations.currentSagaId());
    }

    @Test
    void rejectsATransactionThatDoesNotOwnTheConfiguredSagaDataSource() {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        DataSource dataSource = mock(DataSource.class);
        DefaultSagaTccOperations operations = new DefaultSagaTccOperations(
                coordinator, properties, new MockEnvironment(), dataSource);
        activateTransaction();

        SagaTccException mismatch = assertThrows(SagaTccException.class,
                () -> operations.begin("create-order", "student-1", "wrong-manager"));

        assertTrue(mismatch.getMessage().contains("does not manage the SagaTcc JDBC DataSource"));
        TransactionSynchronizationManager.bindResource(dataSource, new Object());
        try {
            assertEquals("correct-manager",
                    operations.begin("create-order", "student-1", "correct-manager"));
        } finally {
            TransactionSynchronizationManager.unbindResource(dataSource);
        }
    }

    @Test
    void rejectsAnnotationDrivenNestedSavepointBeforeCreatingSagaContext() {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        TransactionStatus nestedStatus = mock(TransactionStatus.class);
        when(nestedStatus.hasSavepoint()).thenReturn(true);
        when(transactionManager.getTransaction(any())).thenReturn(nestedStatus);

        DefaultTransactionAttribute nested = new DefaultTransactionAttribute(
                TransactionDefinition.PROPAGATION_NESTED);
        MatchAlwaysTransactionAttributeSource attributes = new MatchAlwaysTransactionAttributeSource();
        attributes.setTransactionAttribute(nested);
        TransactionInterceptor interceptor = new TransactionInterceptor(transactionManager, attributes);
        ProxyFactory factory = new ProxyFactory();
        factory.setInterfaces(NestedBeginInvoker.class);
        factory.setTarget((NestedBeginInvoker) () ->
                operations.begin("create-order", "student-1", "nested-saga"));
        factory.addAdvice(interceptor);
        NestedBeginInvoker invoker = (NestedBeginInvoker) factory.getProxy();
        activateTransaction();

        SagaTccException error = assertThrows(SagaTccException.class, invoker::invoke);

        assertTrue(error.getMessage().contains("NESTED/savepoint"));
        assertNull(SagaTccContextHolder.get());
        assertTrue(TransactionSynchronizationManager.getSynchronizations().isEmpty());
        verify(transactionManager).rollback(nestedStatus);
    }

    @Test
    void persistenceFailureIsPropagatedAndCompletionStillClearsTheContext() {
        DefaultSagaTccOperations operations = operationsWithBranchLimit(2);
        activateTransaction();
        operations.begin("create-order", "student-1", "saga-failed-commit");
        RuntimeException databaseFailure = new RuntimeException("database unavailable");
        doThrow(databaseFailure).when(coordinator).persistAndScheduleTry(any());

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> synchronization().beforeCommit(false));
        assertEquals(databaseFailure, thrown);

        synchronization().afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);

        assertNull(SagaTccContextHolder.get());
        verify(coordinator, never()).publishPendingOutbox();
    }

    private DefaultSagaTccOperations operationsWithBranchLimit(int maxBranches) {
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        properties.setMaxBranchesPerSaga(maxBranches);
        return new DefaultSagaTccOperations(coordinator, properties, new MockEnvironment());
    }

    private void activateTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
    }

    private TransactionSynchronization synchronization() {
        return TransactionSynchronizationManager.getSynchronizations().get(0);
    }

    private interface NestedBeginInvoker {
        void invoke();
    }

    private static final class PlainRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;

        private final String value;

        private PlainRequest(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
