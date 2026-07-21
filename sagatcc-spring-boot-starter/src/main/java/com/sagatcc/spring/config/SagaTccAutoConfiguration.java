package com.sagatcc.spring.config;

import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.sagatcc.core.api.SagaTccFailureClassifier;
import com.sagatcc.core.api.SagaTccOperations;
import com.sagatcc.core.api.SagaTccParticipant;
import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.spring.coordinator.DefaultSagaTccOperations;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;
import com.sagatcc.spring.idempotent.JdbcParticipantLogRepository;
import com.sagatcc.spring.idempotent.ParticipantLogRepository;
import com.sagatcc.spring.messaging.OutboxPublisherJob;
import com.sagatcc.spring.messaging.RocketMqCommandListener;
import com.sagatcc.spring.messaging.RocketMqResultListener;
import com.sagatcc.spring.messaging.RocketMqSagaMessagePublisher;
import com.sagatcc.spring.messaging.SagaMessagePublisher;
import com.sagatcc.spring.participant.SagaTccParticipantDispatcher;
import com.sagatcc.spring.participant.SagaTccParticipantRegistry;
import com.sagatcc.spring.store.JdbcSagaTccRepository;
import com.sagatcc.spring.store.SagaTccDataSourceProvider;
import com.sagatcc.spring.store.SagaTccRepository;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(SagaTccProperties.class)
public class SagaTccAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper sagaTccObjectMapper() {
        return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Bean
    @ConditionalOnMissingBean(SagaTccRepository.class)
    public SagaTccRepository sagaTccRepository(JdbcTemplate jdbcTemplate, SagaTccProperties properties) {
        return new JdbcSagaTccRepository(jdbcTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean(ParticipantLogRepository.class)
    public ParticipantLogRepository participantLogRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcParticipantLogRepository(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(SagaTccFailureClassifier.class)
    public SagaTccFailureClassifier sagaTccFailureClassifier() {
        return failure -> !(failure instanceof SagaTccNonRetryableException);
    }

    @Bean
    public static BeanFactoryPostProcessor sagaTccTransactionManagerAlias(Environment environment) {
        String configuredName = environment.getProperty(
                "sagatcc.transaction-manager-bean-name", "transactionManager").trim();
        return beanFactory -> {
            if (configuredName.length() == 0 || "sagaTccTransactionManager".equals(configuredName)) {
                throw new IllegalArgumentException("sagatcc.transaction-manager-bean-name must reference the "
                        + "underlying business transaction manager");
            }
            if (!beanFactory.containsBean(configuredName)) {
                throw new IllegalArgumentException("SagaTcc transaction manager bean does not exist: "
                        + configuredName);
            }
            if (beanFactory.containsBean("sagaTccTransactionManager")) {
                for (String alias : beanFactory.getAliases(configuredName)) {
                    if ("sagaTccTransactionManager".equals(alias)) {
                        return;
                    }
                }
                throw new IllegalArgumentException("Bean name 'sagaTccTransactionManager' is reserved for the "
                        + "configured SagaTcc transaction manager alias: " + configuredName);
            }
            beanFactory.registerAlias(configuredName, "sagaTccTransactionManager");
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaMessagePublisher sagaMessagePublisher(RocketMQTemplate rocketMQTemplate,
                                                     SagaTccProperties properties,
                                                     Environment environment) {
        validatePerApplicationTopics(properties, environment);
        return new RocketMqSagaMessagePublisher(rocketMQTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTccCoordinator sagaTccCoordinator(SagaTccRepository repository,
                                                 SagaMessagePublisher publisher,
                                                 ObjectMapper objectMapper,
                                                 SagaTccProperties properties,
                                                 Environment environment,
                                                 @Qualifier("sagaTccTransactionManager")
                                                 PlatformTransactionManager transactionManager) {
        TransactionTemplate recoveryTransaction = new TransactionTemplate(transactionManager);
        recoveryTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        DataSource managedDataSource = managedDataSource(repository);
        return new SagaTccCoordinator(repository, publisher, objectMapper, properties,
                SagaTccNameResolver.applicationName(properties, environment), recoveryTransaction,
                managedDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTccOperations sagaTccOperations(SagaTccCoordinator coordinator,
                                               SagaTccProperties properties,
                                               Environment environment,
                                               SagaTccRepository repository) {
        DataSource managedDataSource = managedDataSource(repository);
        return new DefaultSagaTccOperations(coordinator, properties, environment, managedDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTccParticipantRegistry sagaTccParticipantRegistry(
            ObjectProvider<SagaTccParticipant> participants) {
        return new SagaTccParticipantRegistry(participants.orderedStream().collect(Collectors.toList()));
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaTccParticipantDispatcher sagaTccParticipantDispatcher(SagaTccParticipantRegistry registry,
                                                                     ParticipantLogRepository participantLogRepository,
                                                                     SagaMessagePublisher publisher,
                                                                     ObjectMapper objectMapper,
                                                                     SagaTccFailureClassifier failureClassifier,
                                                                     SagaTccProperties properties,
                                                                     @Qualifier("sagaTccTransactionManager")
                                                                     PlatformTransactionManager transactionManager,
                                                                     Environment environment) {
        TransactionTemplate participantTransaction = new TransactionTemplate(transactionManager);
        participantTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        DataSource managedDataSource = managedDataSource(participantLogRepository);
        return new SagaTccParticipantDispatcher(registry, participantLogRepository, publisher, objectMapper,
                failureClassifier, properties, participantTransaction, environment,
                managedDataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisherJob outboxPublisherJob(SagaTccCoordinator coordinator,
                                                 SagaTccProperties properties,
                                                 @Qualifier("sagaTccWorkerExecutor") Executor executor) {
        return new OutboxPublisherJob(coordinator, properties, executor);
    }

    @Bean(name = "sagaTccWorkerExecutor")
    @ConditionalOnMissingBean(name = "sagaTccWorkerExecutor")
    public ThreadPoolTaskExecutor sagaTccWorkerExecutor(SagaTccProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int threads = properties.getOutboxPublishConcurrency() + 1;
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("sagatcc-worker-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }

    @Bean
    @ConditionalOnMissingBean
    public RocketMqCommandListener rocketMqCommandListener(SagaTccParticipantDispatcher dispatcher) {
        return new RocketMqCommandListener(dispatcher);
    }

    @Bean
    @ConditionalOnMissingBean
    public RocketMqResultListener rocketMqResultListener(ObjectMapper objectMapper, SagaTccCoordinator coordinator,
                                                         SagaTccProperties properties, Environment environment) {
        return new RocketMqResultListener(objectMapper, coordinator,
                SagaTccNameResolver.applicationName(properties, environment), properties.getMaxMessageBytes());
    }

    private void validatePerApplicationTopics(SagaTccProperties properties, Environment environment) {
        if (!properties.getRocketmq().isPerApplicationTopic()) {
            return;
        }
        String applicationName = SagaTccNameResolver.applicationName(properties, environment);
        String expectedCommandTopic = properties.getRocketmq().getCommandTopicPrefix() + applicationName;
        String expectedResultTopic = properties.getRocketmq().getResultTopicPrefix() + applicationName;
        if (!expectedCommandTopic.equals(properties.getRocketmq().getCommandTopic())
                || !expectedResultTopic.equals(properties.getRocketmq().getResultTopic())) {
            throw new IllegalArgumentException("per-application topics must match the local listener topics: "
                    + "sagatcc.rocketmq.command-topic=" + expectedCommandTopic + ", "
                    + "sagatcc.rocketmq.result-topic=" + expectedResultTopic);
        }
    }

    private DataSource managedDataSource(Object persistence) {
        if (!(persistence instanceof SagaTccDataSourceProvider)) {
            return null;
        }
        DataSource dataSource = ((SagaTccDataSourceProvider) persistence).sagaTccDataSource();
        if (dataSource == null) {
            throw new IllegalArgumentException("SagaTccDataSourceProvider must return its transactional DataSource");
        }
        return dataSource;
    }

}
