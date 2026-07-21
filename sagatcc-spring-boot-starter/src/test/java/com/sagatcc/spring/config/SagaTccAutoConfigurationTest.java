package com.sagatcc.spring.config;

import java.util.Collections;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccOperations;
import com.sagatcc.spring.coordinator.DefaultSagaTccOperations;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;
import com.sagatcc.spring.idempotent.ParticipantLogRepository;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.participant.SagaTccParticipantDispatcher;
import com.sagatcc.spring.participant.SagaTccParticipantRegistry;
import com.sagatcc.spring.store.SagaTccDataSourceProvider;
import com.sagatcc.spring.store.SagaTccRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.aop.support.AopUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

class SagaTccAutoConfigurationTest {

    @Test
    void transactionManagerAliasUsesTheSameSingletonWithoutAddingASecondCandidate() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        beanFactory.registerSingleton("businessTx", transactionManager);
        MockEnvironment environment = new MockEnvironment()
                .withProperty("sagatcc.transaction-manager-bean-name", "businessTx");

        BeanFactoryPostProcessor alias = SagaTccAutoConfiguration.sagaTccTransactionManagerAlias(environment);
        alias.postProcessBeanFactory(beanFactory);

        assertThat(beanFactory.getBean("sagaTccTransactionManager")).isSameAs(transactionManager);
        assertThat(beanFactory.getBeanNamesForType(PlatformTransactionManager.class)).containsExactly("businessTx");
    }

    @Test
    void missingConfiguredTransactionManagerFailsBeforeConsumersCanStart() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        MockEnvironment environment = new MockEnvironment()
                .withProperty("sagatcc.transaction-manager-bean-name", "missingTx");

        assertThatThrownBy(() -> SagaTccAutoConfiguration.sagaTccTransactionManagerAlias(environment)
                .postProcessBeanFactory(beanFactory))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missingTx");
    }

    @Test
    void preExistingCorrectAliasIsIdempotent() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        beanFactory.registerSingleton("businessTx", transactionManager);
        beanFactory.registerAlias("businessTx", "sagaTccTransactionManager");
        MockEnvironment environment = new MockEnvironment()
                .withProperty("sagatcc.transaction-manager-bean-name", "businessTx");

        SagaTccAutoConfiguration.sagaTccTransactionManagerAlias(environment)
                .postProcessBeanFactory(beanFactory);

        assertThat(beanFactory.getBean("sagaTccTransactionManager")).isSameAs(transactionManager);
        assertThat(beanFactory.getBeanNamesForType(PlatformTransactionManager.class)).containsExactly("businessTx");
    }

    @Test
    void conflictingReservedTransactionManagerNameFailsInsteadOfSilentlyUsingTheWrongManager() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerSingleton("businessTx", mock(PlatformTransactionManager.class));
        beanFactory.registerSingleton("sagaTccTransactionManager", mock(PlatformTransactionManager.class));
        MockEnvironment environment = new MockEnvironment()
                .withProperty("sagatcc.transaction-manager-bean-name", "businessTx");

        assertThatThrownBy(() -> SagaTccAutoConfiguration.sagaTccTransactionManagerAlias(environment)
                .postProcessBeanFactory(beanFactory))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved")
                .hasMessageContaining("businessTx");
    }

    @Test
    void customPersistenceExtensionsAreNotForcedToUseTheAutoConfiguredJdbcDataSource() {
        SagaTccAutoConfiguration configuration = new SagaTccAutoConfiguration();
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        MockEnvironment environment = new MockEnvironment();
        SagaTccRepository customCoordinatorStore = mock(SagaTccRepository.class);
        ParticipantLogRepository customParticipantStore = mock(ParticipantLogRepository.class);
        SagaMessagePublisher publisher = mock(SagaMessagePublisher.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        SagaTccCoordinator coordinator = configuration.sagaTccCoordinator(customCoordinatorStore, publisher,
                new ObjectMapper(), properties, environment, transactionManager);
        SagaTccOperations operations = configuration.sagaTccOperations(coordinator, properties, environment,
                customCoordinatorStore);
        SagaTccParticipantDispatcher dispatcher = configuration.sagaTccParticipantDispatcher(
                new SagaTccParticipantRegistry(Collections.emptyList()), customParticipantStore, publisher,
                new ObjectMapper(), failure -> true, properties, transactionManager, environment);

        assertThat(ReflectionTestUtils.getField(coordinator, "dataSource")).isNull();
        assertThat(ReflectionTestUtils.getField((DefaultSagaTccOperations) operations, "dataSource")).isNull();
        assertThat(ReflectionTestUtils.getField(dispatcher, "dataSource")).isNull();
        assertThat(((TransactionTemplate) ReflectionTestUtils.getField(coordinator, "recoveryTransaction"))
                .getPropagationBehavior()).isEqualTo(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(((TransactionTemplate) ReflectionTestUtils.getField(dispatcher, "transactionTemplate"))
                .getPropagationBehavior()).isEqualTo(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Test
    void jdbcBackedExtensionCanExposeItsExactTransactionalDataSourceThroughSpi() {
        SagaTccAutoConfiguration configuration = new SagaTccAutoConfiguration();
        SagaTccProperties properties = new SagaTccProperties();
        properties.setApplicationName("order");
        MockEnvironment environment = new MockEnvironment();
        DataSource coordinatorDataSource = mock(DataSource.class);
        DataSource participantDataSource = mock(DataSource.class);
        SagaTccRepository coordinatorStore = mock(SagaTccRepository.class,
                withSettings().extraInterfaces(SagaTccDataSourceProvider.class));
        ParticipantLogRepository participantStore = mock(ParticipantLogRepository.class,
                withSettings().extraInterfaces(SagaTccDataSourceProvider.class));
        when(((SagaTccDataSourceProvider) coordinatorStore).sagaTccDataSource())
                .thenReturn(coordinatorDataSource);
        when(((SagaTccDataSourceProvider) participantStore).sagaTccDataSource())
                .thenReturn(participantDataSource);
        SagaMessagePublisher publisher = mock(SagaMessagePublisher.class);
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);

        SagaTccCoordinator coordinator = configuration.sagaTccCoordinator(coordinatorStore, publisher,
                new ObjectMapper(), properties, environment, transactionManager);
        SagaTccOperations operations = configuration.sagaTccOperations(
                coordinator, properties, environment, coordinatorStore);
        SagaTccParticipantDispatcher dispatcher = configuration.sagaTccParticipantDispatcher(
                new SagaTccParticipantRegistry(Collections.emptyList()), participantStore, publisher,
                new ObjectMapper(), failure -> true, properties, transactionManager, environment);

        assertThat(ReflectionTestUtils.getField(coordinator, "dataSource")).isSameAs(coordinatorDataSource);
        assertThat(ReflectionTestUtils.getField((DefaultSagaTccOperations) operations, "dataSource"))
                .isSameAs(coordinatorDataSource);
        assertThat(ReflectionTestUtils.getField(dispatcher, "dataSource")).isSameAs(participantDataSource);
    }

    @Test
    void nonJdbcPersistenceExtensionsCanStartWithoutJdbcTemplateOrDataSource() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        TransactionAutoConfiguration.class, SagaTccAutoConfiguration.class))
                .withPropertyValues("sagatcc.application-name=order", "sagatcc.scheduler-enabled=false")
                .withBean("transactionManager", PlatformTransactionManager.class,
                        () -> mock(PlatformTransactionManager.class))
                .withBean(SagaTccRepository.class, () -> mock(SagaTccRepository.class))
                .withBean(ParticipantLogRepository.class, () -> mock(ParticipantLogRepository.class))
                .withBean(SagaMessagePublisher.class, () -> mock(SagaMessagePublisher.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(JdbcTemplate.class);
                    assertThat(context).hasSingleBean(SagaTccCoordinator.class);
                    assertThat(context).hasSingleBean(SagaTccParticipantDispatcher.class);
                    assertThat(AopUtils.isAopProxy(context.getBean(SagaTccCoordinator.class))).isTrue();
                    assertThat(context.getBean("sagaTccTransactionManager"))
                            .isSameAs(context.getBean("transactionManager"));
                    assertThat(context.getBeanNamesForType(PlatformTransactionManager.class))
                            .containsExactly("transactionManager");
                });
    }
}
