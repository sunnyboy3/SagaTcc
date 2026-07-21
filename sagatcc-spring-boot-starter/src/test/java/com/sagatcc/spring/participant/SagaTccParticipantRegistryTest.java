package com.sagatcc.spring.participant;

import java.util.Arrays;
import java.util.Collections;

import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccParticipant;
import com.sagatcc.core.api.SagaTccRequest;

import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SagaTccParticipantRegistryTest {

    @Test
    void routingAnnotationMayLiveOnParticipant() {
        PlainParticipant participant = new PlainParticipant();
        SagaTccParticipantRegistry registry = registry(participant);

        SagaTccParticipantRegistry.ParticipantMeta meta = registry.find("wallet", "pay");

        assertNotNull(meta);
        assertSame(participant, meta.getParticipant());
        assertEquals(PlainRequest.class, meta.getRequestClass());
    }

    @Test
    void routingAnnotationMayLiveOnRequestWithoutAnnotatingBusinessBean() {
        RequestAnnotatedParticipant participant = new RequestAnnotatedParticipant();
        SagaTccParticipantRegistry registry = registry(participant);

        SagaTccParticipantRegistry.ParticipantMeta meta = registry.find("inventory", "reserve");

        assertNotNull(meta);
        assertSame(participant, meta.getParticipant());
        assertEquals(RequestAnnotated.class, meta.getRequestClass());
    }

    @Test
    void matchingAnnotationsOnRequestAndParticipantAreAccepted() {
        BothAnnotatedParticipant participant = new BothAnnotatedParticipant();

        SagaTccParticipantRegistry.ParticipantMeta meta = registry(participant).find("school", "enroll");

        assertNotNull(meta);
        assertEquals(BothAnnotatedRequest.class, meta.getRequestClass());
    }

    @Test
    void conflictingAnnotationsFailFastAtStartup() {
        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> registry(new ConflictingParticipant()));

        assertTrue(failure.getMessage().contains("conflicting @SagaTccBusiness declarations"));
    }

    @Test
    void participantWithoutAnyRoutingAnnotationFailsFast() {
        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> registry(new MissingAnnotationParticipant()));

        assertTrue(failure.getMessage().contains("@SagaTccBusiness is required"));
    }

    @Test
    void duplicateRouteCannotSilentlyOverrideTheFirstBean() {
        RequestAnnotatedParticipant first = new RequestAnnotatedParticipant();
        AnotherRequestAnnotatedParticipant second = new AnotherRequestAnnotatedParticipant();

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> new SagaTccParticipantRegistry(Arrays.<SagaTccParticipant>asList(first, second)));

        assertTrue(failure.getMessage().contains("duplicated SagaTcc participant"));
        assertTrue(failure.getMessage().contains("inventory:reserve"));
    }

    @Test
    void whitespaceInAnnotationKeysIsNormalizedConsistently() {
        SagaTccParticipantRegistry registry = registry(new WhitespaceParticipant());

        assertNotNull(registry.find("wallet", "refund"));
        assertNotNull(registry.find(" wallet ", " refund "));
    }

    @Test
    void emptyRegistryAndUnknownRoutesReturnNull() {
        SagaTccParticipantRegistry empty = new SagaTccParticipantRegistry(null);

        assertNull(empty.find("wallet", "pay"));
        assertNull(registry(new PlainParticipant()).find("wallet", "missing"));
    }

    @Test
    void lookupValidatesNullBlankOversizedAndColonKeys() {
        SagaTccParticipantRegistry registry = new SagaTccParticipantRegistry(
                Collections.<SagaTccParticipant>emptyList());

        assertThrows(SagaTccException.class, () -> registry.find(null, "pay"));
        assertThrows(SagaTccException.class, () -> registry.find(" ", "pay"));
        assertThrows(SagaTccException.class, () -> registry.find("wallet", null));
        assertThrows(SagaTccException.class, () -> registry.find("wallet", "\t"));
        assertThrows(SagaTccException.class, () -> registry.find(repeat('a', 129), "pay"));
        assertThrows(SagaTccException.class, () -> registry.find("wallet", repeat('b', 129)));
        assertThrows(SagaTccException.class, () -> registry.find("wallet:blue", "pay"));
        assertNull(registry.find(repeat('a', 128), repeat('b', 128)),
                "the documented inclusive maximum must be accepted");
    }

    @Test
    void invalidAnnotationRouteFailsDuringRegistryConstruction() {
        SagaTccException colon = assertThrows(SagaTccException.class,
                () -> registry(new ColonAppParticipant()));
        SagaTccException blank = assertThrows(SagaTccException.class,
                () -> registry(new BlankBusinessParticipant()));

        assertTrue(colon.getMessage().contains("RocketMQ-safe characters"));
        assertTrue(blank.getMessage().contains("must contain 1 to 128 characters"));
    }

    @Test
    void inheritedGenericParticipantResolvesItsConcreteRequestType() {
        InheritedParticipant participant = new InheritedParticipant();

        SagaTccParticipantRegistry.ParticipantMeta meta = registry(participant)
                .find("inventory", "reserve");

        assertNotNull(meta);
        assertEquals(RequestAnnotated.class, meta.getRequestClass());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void rawParticipantTypeIsRejectedInsteadOfRegisteringAnUnsafeHandler() {
        SagaTccParticipant raw = new RawParticipant();

        SagaTccException failure = assertThrows(SagaTccException.class,
                () -> registry(raw));

        assertTrue(failure.getMessage().contains("can not resolve SagaTccParticipant request type"));
    }

    @Test
    void jdkProxyUsesTargetClassForGenericAndAnnotationResolution() {
        RequestAnnotatedParticipant target = new RequestAnnotatedParticipant();
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(false);
        SagaTccParticipant proxy = (SagaTccParticipant) factory.getProxy();

        SagaTccParticipantRegistry.ParticipantMeta meta = registry(proxy)
                .find("inventory", "reserve");

        assertNotNull(meta);
        assertSame(proxy, meta.getParticipant());
        assertEquals(RequestAnnotated.class, meta.getRequestClass());
    }

    @Test
    void cglibProxyUsesTargetClassForGenericAndAnnotationResolution() {
        PlainParticipant target = new PlainParticipant();
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(true);
        SagaTccParticipant proxy = (SagaTccParticipant) factory.getProxy();

        SagaTccParticipantRegistry.ParticipantMeta meta = registry(proxy).find("wallet", "pay");

        assertNotNull(meta);
        assertSame(proxy, meta.getParticipant());
        assertEquals(PlainRequest.class, meta.getRequestClass());
    }

    private static SagaTccParticipantRegistry registry(SagaTccParticipant participant) {
        return new SagaTccParticipantRegistry(Collections.singletonList(participant));
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }

    private static class PlainRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;
    }

    @SagaTccBusiness(appId = "wallet", busCode = "pay")
    static class PlainParticipant implements SagaTccParticipant<PlainRequest> {
        @Override
        public void sagaTry(PlainRequest request) {
        }

        @Override
        public void sagaConfirm(PlainRequest request) {
        }

        @Override
        public void sagaCancel(PlainRequest request) {
        }
    }

    @SagaTccBusiness(appId = "inventory", busCode = "reserve")
    private static class RequestAnnotated implements SagaTccRequest {
        private static final long serialVersionUID = 1L;
    }

    static class RequestAnnotatedParticipant implements SagaTccParticipant<RequestAnnotated> {
        @Override
        public void sagaTry(RequestAnnotated request) {
        }

        @Override
        public void sagaConfirm(RequestAnnotated request) {
        }

        @Override
        public void sagaCancel(RequestAnnotated request) {
        }
    }

    static class AnotherRequestAnnotatedParticipant extends RequestAnnotatedParticipant {
    }

    @SagaTccBusiness(appId = "school", busCode = "enroll")
    private static class BothAnnotatedRequest implements SagaTccRequest {
        private static final long serialVersionUID = 1L;
    }

    @SagaTccBusiness(appId = "school", busCode = "enroll")
    private static class BothAnnotatedParticipant implements SagaTccParticipant<BothAnnotatedRequest> {
        @Override
        public void sagaTry(BothAnnotatedRequest request) {
        }

        @Override
        public void sagaConfirm(BothAnnotatedRequest request) {
        }

        @Override
        public void sagaCancel(BothAnnotatedRequest request) {
        }
    }

    @SagaTccBusiness(appId = "wallet", busCode = "pay")
    private static class ConflictingParticipant implements SagaTccParticipant<RequestAnnotated> {
        @Override
        public void sagaTry(RequestAnnotated request) {
        }

        @Override
        public void sagaConfirm(RequestAnnotated request) {
        }

        @Override
        public void sagaCancel(RequestAnnotated request) {
        }
    }

    private static class MissingAnnotationParticipant implements SagaTccParticipant<PlainRequest> {
        @Override
        public void sagaTry(PlainRequest request) {
        }

        @Override
        public void sagaConfirm(PlainRequest request) {
        }

        @Override
        public void sagaCancel(PlainRequest request) {
        }
    }

    @SagaTccBusiness(appId = " wallet ", busCode = " refund ")
    private static class WhitespaceParticipant implements SagaTccParticipant<PlainRequest> {
        @Override
        public void sagaTry(PlainRequest request) {
        }

        @Override
        public void sagaConfirm(PlainRequest request) {
        }

        @Override
        public void sagaCancel(PlainRequest request) {
        }
    }

    @SagaTccBusiness(appId = "wallet:blue", busCode = "pay")
    private static class ColonAppParticipant implements SagaTccParticipant<PlainRequest> {
        @Override
        public void sagaTry(PlainRequest request) {
        }

        @Override
        public void sagaConfirm(PlainRequest request) {
        }

        @Override
        public void sagaCancel(PlainRequest request) {
        }
    }

    @SagaTccBusiness(appId = "wallet", busCode = " ")
    private static class BlankBusinessParticipant implements SagaTccParticipant<PlainRequest> {
        @Override
        public void sagaTry(PlainRequest request) {
        }

        @Override
        public void sagaConfirm(PlainRequest request) {
        }

        @Override
        public void sagaCancel(PlainRequest request) {
        }
    }

    private abstract static class GenericParticipant<T extends SagaTccRequest>
            implements SagaTccParticipant<T> {
        @Override
        public void sagaTry(T request) {
        }

        @Override
        public void sagaConfirm(T request) {
        }

        @Override
        public void sagaCancel(T request) {
        }
    }

    private static class InheritedParticipant extends GenericParticipant<RequestAnnotated> {
    }

    @SagaTccBusiness(appId = "raw", busCode = "unsafe")
    @SuppressWarnings("rawtypes")
    private static class RawParticipant implements SagaTccParticipant {
        @Override
        public void sagaTry(SagaTccRequest request) {
        }

        @Override
        public void sagaConfirm(SagaTccRequest request) {
        }

        @Override
        public void sagaCancel(SagaTccRequest request) {
        }
    }
}
