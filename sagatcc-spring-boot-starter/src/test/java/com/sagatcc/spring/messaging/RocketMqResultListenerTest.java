package com.sagatcc.spring.messaging;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.message.SagaTccAction;
import com.sagatcc.core.message.SagaTccResultMessage;
import com.sagatcc.spring.coordinator.SagaTccCoordinator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RocketMqResultListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SagaTccCoordinator coordinator = mock(SagaTccCoordinator.class);

    @Test
    void localResultReachesTransactionalCoordinator() throws Exception {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");
        String payload = objectMapper.writeValueAsString(result("order"));
        assertThat(payload).doesNotContain("Specified");

        listener.onMessage(payload);

        ArgumentCaptor<SagaTccResultMessage> captured = ArgumentCaptor.forClass(SagaTccResultMessage.class);
        verify(coordinator).handleResult(captured.capture());
        assertThat(captured.getValue().getSagaId()).isEqualTo("saga-1");
        assertThat(captured.getValue().getBranchId()).isEqualTo(9L);
    }

    @Test
    void foreignApplicationResultIsFilteredBeforeOpeningCoordinatorTransaction() throws Exception {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");

        listener.onMessage(objectMapper.writeValueAsString(result("another-coordinator")));

        verifyNoInteractions(coordinator);
    }

    @Test
    void structurallyIncompleteResultIsAcknowledgedWithoutOpeningTransaction() {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");

        listener.onMessage("{\"coordinatorApp\":\"order\"}");

        verifyNoInteractions(coordinator);
    }

    @Test
    void missingBooleanProtocolFlagsCannotBeMisreadAsPermanentBusinessFailure() {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");
        String identity = "\"sagaId\":\"saga-1\",\"branchId\":9,\"coordinatorApp\":\"order\"," +
                "\"targetApp\":\"wallet\",\"busCode\":\"reserve\",\"action\":\"TRY\",\"attempt\":1";

        listener.onMessage("{" + identity + "}");
        listener.onMessage("{" + identity + ",\"success\":false}");
        listener.onMessage("{" + identity + ",\"success\":null,\"retryable\":null}");

        verifyNoInteractions(coordinator);
    }

    @Test
    void invalidIdentityBoundariesAreFilteredBeforeCoordinatorTransaction() throws Exception {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");
        List<SagaTccResultMessage> invalid = Arrays.asList(
                mutate(result("order"), value -> value.setBranchId(0L)),
                mutate(result("order"), value -> value.setAttempt(0)),
                mutate(result("order"), value -> value.setSagaId(" ")),
                mutate(result("order"), value -> value.setSagaId(repeat('s', 65))),
                mutate(result("order"), value -> value.setTargetApp("")),
                mutate(result("order"), value -> value.setBusCode(repeat('b', 129)))
        );

        for (SagaTccResultMessage value : invalid) {
            listener.onMessage(objectMapper.writeValueAsString(value));
        }

        verifyNoInteractions(coordinator);
    }

    @Test
    void malformedJsonStillFailsDeliveryForBrokerRetryOrDlq() {
        RocketMqResultListener listener = new RocketMqResultListener(objectMapper, coordinator, "order");

        assertThatThrownBy(() -> listener.onMessage("not-json"))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining("handle SagaTcc result failed");
        verifyNoInteractions(coordinator);
    }

    @Test
    void oversizedEnvelopeIsRejectedBeforeJsonParsingOrCoordinatorTransaction() throws Exception {
        String payload = objectMapper.writeValueAsString(result("order"));
        RocketMqResultListener listener = new RocketMqResultListener(
                objectMapper, coordinator, "order", payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length - 1);

        assertThatThrownBy(() -> listener.onMessage(payload))
                .isInstanceOf(SagaTccException.class)
                .hasMessageContaining("max-message-bytes");
        verifyNoInteractions(coordinator);
    }

    private SagaTccResultMessage result(String coordinatorApp) {
        SagaTccResultMessage result = new SagaTccResultMessage();
        result.setMessageKey("saga-1-9-TRY-1-result");
        result.setSagaId("saga-1");
        result.setBranchId(9L);
        result.setCoordinatorApp(coordinatorApp);
        result.setTargetApp("wallet");
        result.setBusCode("reserve");
        result.setAction(SagaTccAction.TRY);
        result.setAttempt(1);
        result.setSuccess(true);
        result.setRetryable(false);
        return result;
    }

    private SagaTccResultMessage mutate(SagaTccResultMessage value,
                                        java.util.function.Consumer<SagaTccResultMessage> mutation) {
        mutation.accept(value);
        return value;
    }

    private String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }
}
