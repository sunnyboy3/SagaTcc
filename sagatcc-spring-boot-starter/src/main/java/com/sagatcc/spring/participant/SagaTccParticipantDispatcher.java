package com.sagatcc.spring.participant;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccFailureClassifier;
import com.sagatcc.core.api.SagaTccNonRetryableException;
import com.sagatcc.core.api.SagaTccParticipant;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccCommandMessage;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.spring.config.SagaTccNameResolver;
import com.sagatcc.spring.config.SagaTccProperties;
import com.sagatcc.spring.idempotent.JdbcParticipantLogRepository;
import com.sagatcc.spring.idempotent.ParticipantLogRepository;
import com.sagatcc.spring.messaging.SagaMessagePublisher;

import org.springframework.core.env.Environment;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class SagaTccParticipantDispatcher {

    private final SagaTccParticipantRegistry registry;
    private final ParticipantLogRepository participantLogRepository;
    private final SagaMessagePublisher publisher;
    private final ObjectMapper objectMapper;
    private final SagaTccFailureClassifier failureClassifier;
    private final TransactionTemplate transactionTemplate;
    private final String localApplication;
    private final DataSource dataSource;
    private final int maxRequestBytes;
    private final int maxMessageBytes;

    public SagaTccParticipantDispatcher(SagaTccParticipantRegistry registry,
                                        ParticipantLogRepository participantLogRepository,
                                        SagaMessagePublisher publisher,
                                        ObjectMapper objectMapper,
                                        SagaTccFailureClassifier failureClassifier,
                                        SagaTccProperties properties,
                                        TransactionTemplate transactionTemplate,
                                        Environment environment) {
        this(registry, participantLogRepository, publisher, objectMapper, failureClassifier,
                properties, transactionTemplate, environment, null);
    }

    public SagaTccParticipantDispatcher(SagaTccParticipantRegistry registry,
                                        ParticipantLogRepository participantLogRepository,
                                        SagaMessagePublisher publisher,
                                        ObjectMapper objectMapper,
                                        SagaTccFailureClassifier failureClassifier,
                                        SagaTccProperties properties,
                                        TransactionTemplate transactionTemplate,
                                        Environment environment,
                                        DataSource dataSource) {
        this.registry = registry;
        this.participantLogRepository = participantLogRepository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.failureClassifier = failureClassifier;
        this.transactionTemplate = transactionTemplate;
        this.localApplication = SagaTccNameResolver.applicationName(properties, environment);
        this.dataSource = dataSource;
        this.maxRequestBytes = properties.getMaxRequestBytes();
        this.maxMessageBytes = properties.getMaxMessageBytes();
    }

    /** @deprecated use the constructor accepting the participant log interface and failure classifier. */
    @Deprecated
    public SagaTccParticipantDispatcher(SagaTccParticipantRegistry registry,
                                        JdbcParticipantLogRepository participantLogRepository,
                                        SagaMessagePublisher publisher,
                                        ObjectMapper objectMapper,
                                        SagaTccProperties properties,
                                        TransactionTemplate transactionTemplate,
                                        Environment environment) {
        this(registry, participantLogRepository, publisher, objectMapper, failure -> true,
                properties, transactionTemplate, environment);
    }

    public void dispatch(String payload) {
        try {
            if (payload == null || payload.getBytes(StandardCharsets.UTF_8).length > maxMessageBytes) {
                throw new SagaTccException("SagaTcc command exceeds max-message-bytes");
            }
            SagaTccCommandMessage command = objectMapper.readValue(payload, SagaTccCommandMessage.class);
            validate(command);
            if (!localApplication.equals(command.getTargetApp())) {
                return;
            }
            dispatch(command);
        } catch (SagaTccException e) {
            throw e;
        } catch (Exception e) {
            throw new SagaTccException("parse SagaTcc command failed", e);
        }
    }

    private void dispatch(SagaTccCommandMessage command) {
        try {
            SagaTccParticipantRegistry.ParticipantMeta meta = registry.find(command.getTargetApp(), command.getBusCode());
            if (meta == null) {
                throw new SagaTccException("no SagaTcc participant for " + command.getTargetApp() + ":" + command.getBusCode());
            }
            if (!meta.getRequestClass().getName().equals(command.getRequestClass())) {
                throw new SagaTccNonRetryableException("request class does not match registered participant, expected="
                        + meta.getRequestClass().getName() + ", actual=" + command.getRequestClass());
            }
            if (command.getRequestJson().getBytes(StandardCharsets.UTF_8).length > maxRequestBytes) {
                throw new SagaTccNonRetryableException("request JSON exceeds max-request-bytes");
            }
            SagaTccRequest request;
            try {
                request = (SagaTccRequest) objectMapper.readValue(command.getRequestJson(), meta.getRequestClass());
            } catch (JsonProcessingException e) {
                throw new SagaTccNonRetryableException("request JSON cannot be deserialized", e);
            }
            invoke(localApplication, command, meta.getParticipant(), request);
        } catch (Exception e) {
            publishResult(command, false, e);
            return;
        }
        publishResult(command, true, null);
    }

    private void invoke(String localApp, SagaTccCommandMessage command,
                        SagaTccParticipant participant, SagaTccRequest request) throws Exception {
        transactionTemplate.execute(status -> {
            try {
                if (dataSource != null && (!TransactionSynchronizationManager.isActualTransactionActive()
                        || !TransactionSynchronizationManager.hasResource(dataSource))) {
                    throw new SagaTccException(
                            "SagaTcc transaction manager does not manage the participant JDBC DataSource");
                }
                participantLogRepository.executeIdempotently(localApp, command, new Callable<Void>() {
                    @Override
                    public Void call() {
                        invokeParticipant(command.getAction(), participant, request);
                        return null;
                    }
                });
                return null;
            } catch (Exception e) {
                throw new SagaTccException("invoke SagaTcc participant failed", e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void invokeParticipant(SagaTccAction action, SagaTccParticipant participant, SagaTccRequest request) {
        if (action == SagaTccAction.TRY) {
            participant.sagaTry(request);
        } else if (action == SagaTccAction.CONFIRM) {
            participant.sagaConfirm(request);
        } else if (action == SagaTccAction.CANCEL) {
            participant.sagaCancel(request);
        } else {
            throw new SagaTccException("unsupported SagaTcc action: " + action);
        }
    }

    private void publishResult(SagaTccCommandMessage command, boolean success, Exception error) {
        try {
            SagaTccResultMessage result = new SagaTccResultMessage();
            result.setSagaId(command.getSagaId());
            result.setBranchId(command.getBranchId());
            result.setCoordinatorApp(command.getCoordinatorApp());
            result.setTargetApp(command.getTargetApp());
            result.setBusCode(command.getBusCode());
            result.setAction(command.getAction());
            result.setAttempt(command.getAttempt());
            result.setSuccess(success);
            result.setRetryable(!success && isRetryable(error));
            if (error != null) {
                Throwable cause = rootCause(error);
                result.setErrorCode(cause.getClass().getName());
                result.setErrorMessage(trim(cause.getMessage(), 2000));
            }
            result.setMessageKey(command.getSagaId() + "-" + command.getBranchId() + "-" + command.getAction()
                    + "-" + command.getAttempt() + "-result");
            String json = objectMapper.writeValueAsString(result);
            publisher.publishRaw(publisher.resultTopic(command.getCoordinatorApp()), SagaMessagePublisher.RESULT_TAG,
                    result.getMessageKey(), json);
        } catch (Exception e) {
            throw new SagaTccException("publish SagaTcc result failed", e);
        }
    }

    private void validate(SagaTccCommandMessage command) {
        if (command == null || command.getBranchId() == null || command.getBranchId() <= 0
                || command.getAction() == null || command.getAttempt() <= 0) {
            throw new SagaTccException("invalid SagaTcc command identity or action");
        }
        requireText(command.getMessageKey(), "messageKey", 160);
        requireText(command.getSagaId(), "sagaId", 64);
        requireText(command.getCoordinatorApp(), "coordinatorApp", 128);
        requireText(command.getTargetApp(), "targetApp", 128);
        SagaTccNameResolver.validateApplicationName(command.getCoordinatorApp());
        SagaTccNameResolver.validateApplicationName(command.getTargetApp());
        requireText(command.getBusCode(), "busCode", 128);
        requireText(command.getRequestClass(), "requestClass", 512);
        if (command.getRequestJson() == null) {
            throw new SagaTccException("requestJson must not be null");
        }
    }

    private void requireText(String value, String field, int maxLength) {
        if (value == null || value.trim().length() == 0 || value.length() > maxLength) {
            throw new SagaTccException("invalid SagaTcc command " + field);
        }
    }

    private boolean isRetryable(Throwable error) {
        try {
            Throwable current = error;
            while (current != null) {
                if (current instanceof SagaTccNonRetryableException) {
                    return false;
                }
                current = current.getCause();
            }
            return failureClassifier.isRetryable(rootCause(error));
        } catch (RuntimeException ignored) {
            return true;
        }
    }

    private Throwable rootCause(Throwable error) {
        Throwable result = error;
        while (result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
        }
        return result;
    }

    private String trim(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
