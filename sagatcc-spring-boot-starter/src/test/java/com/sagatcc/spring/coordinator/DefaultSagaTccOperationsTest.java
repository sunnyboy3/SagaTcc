package com.sagatcc.spring.coordinator;

import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.context.SagaTccContext;
import com.sagatcc.core.context.SagaTccContextHolder;
import com.sagatcc.spring.config.SagaTccProperties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DefaultSagaTccOperationsTest {

    private SagaTccCoordinator coordinator;
    private DefaultSagaTccOperations operations;

    @BeforeEach
    void setUp() {
        coordinator = mock(SagaTccCoordinator.class);
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        operations = new DefaultSagaTccOperations(coordinator, properties, new MockEnvironment());
    }

    @AfterEach
    void cleanUpThreadState() {
        SagaTccContextHolder.clear();
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
    }

    @Test
    void beginFailsFastOutsideATransaction() {
        assertThrows(SagaTccException.class, () -> operations.begin("createOrder", "1"));
    }

    @Test
    void explicitRouteKeepsBusinessDtoFreeOfRoutingAnnotation() {
        activateTransaction();
        assertEquals("saga-1", operations.begin(" createOrder ", " 1 ", " saga-1 "));

        operations.enlist(" wallet ", " pay ", new PlainRequest());
        SagaTccContext context = SagaTccContextHolder.get();
        assertEquals("wallet", context.getEnlistments().get(0).getTargetApplication());
        assertEquals("pay", context.getEnlistments().get(0).getBusinessCode());

        TransactionSynchronization synchronization = TransactionSynchronizationManager.getSynchronizations().get(0);
        synchronization.beforeCommit(false);
        synchronization.afterCommit();
        synchronization.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);

        verify(coordinator).persistAndScheduleTry(context);
        verify(coordinator, never()).publishPendingOutbox();
        assertEquals(null, SagaTccContextHolder.get());
    }

    @Test
    void annotationRouteRemainsBackwardCompatible() {
        activateTransaction();
        operations.begin("createOrder", "1", "saga-2");
        operations.enlist(new AnnotatedRequest());

        SagaTccContext context = SagaTccContextHolder.get();
        assertEquals("wallet", context.getEnlistments().get(0).getTargetApplication());
        assertEquals("pay", context.getEnlistments().get(0).getBusinessCode());
    }

    private void activateTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
    }

    private static class PlainRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;
    }

    @SagaTccBusiness(appId = "wallet", busCode = "pay")
    private static class AnnotatedRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;
    }
}
