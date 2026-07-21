package com.sagatcc.spring.participant;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccException;
import com.sagatcc.core.api.SagaTccParticipant;
import com.sagatcc.core.api.SagaTccRequest;
import com.sagatcc.spring.config.SagaTccNameResolver;

import org.springframework.aop.support.AopUtils;
import org.springframework.core.ResolvableType;

public class SagaTccParticipantRegistry {

    private final Map<ParticipantKey, ParticipantMeta> participants;

    public SagaTccParticipantRegistry(List<SagaTccParticipant> participantBeans) {
        Map<ParticipantKey, ParticipantMeta> map = new HashMap<ParticipantKey, ParticipantMeta>();
        if (participantBeans != null) {
            for (SagaTccParticipant participant : participantBeans) {
                Class<?> participantClass = AopUtils.getTargetClass(participant);
                Class<?> requestClass = resolveRequestClass(participantClass);
                SagaTccBusiness business = requestClass.getAnnotation(SagaTccBusiness.class);
                SagaTccBusiness participantBusiness = participantClass.getAnnotation(SagaTccBusiness.class);
                if (business == null) {
                    business = participantBusiness;
                } else if (participantBusiness != null
                        && (!business.appId().equals(participantBusiness.appId())
                        || !business.busCode().equals(participantBusiness.busCode()))) {
                    throw new SagaTccException("conflicting @SagaTccBusiness declarations on participant and request: "
                            + participantClass.getName());
                }
                if (business == null) {
                    throw new SagaTccException("@SagaTccBusiness is required on participant or request: "
                            + participantClass.getName());
                }
                ParticipantKey key = key(business.appId(), business.busCode());
                if (map.containsKey(key)) {
                    throw new SagaTccException("duplicated SagaTcc participant: " + key);
                }
                map.put(key, new ParticipantMeta(participant, requestClass));
            }
        }
        this.participants = Collections.unmodifiableMap(map);
    }

    public ParticipantMeta find(String appId, String busCode) {
        return participants.get(key(appId, busCode));
    }

    private ParticipantKey key(String appId, String busCode) {
        String normalizedAppId = requireText(appId, "appId");
        SagaTccNameResolver.validateApplicationName(normalizedAppId);
        return new ParticipantKey(normalizedAppId, requireText(busCode, "busCode"));
    }

    private Class<?> resolveRequestClass(Class<?> participantClass) {
        ResolvableType type = ResolvableType.forClass(participantClass).as(SagaTccParticipant.class);
        Class<?> clazz = type.getGeneric(0).resolve();
        if (clazz == null || clazz == SagaTccRequest.class || clazz.isInterface()
                || !SagaTccRequest.class.isAssignableFrom(clazz)) {
            throw new SagaTccException("can not resolve SagaTccParticipant request type: " + participantClass);
        }
        return clazz;
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().length() == 0 || value.trim().length() > 128) {
            throw new SagaTccException("SagaTcc participant " + field + " must contain 1 to 128 characters");
        }
        return value.trim();
    }

    private static final class ParticipantKey {

        private final String appId;
        private final String busCode;

        private ParticipantKey(String appId, String busCode) {
            this.appId = appId;
            this.busCode = busCode;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ParticipantKey)) {
                return false;
            }
            ParticipantKey that = (ParticipantKey) other;
            return appId.equals(that.appId) && busCode.equals(that.busCode);
        }

        @Override
        public int hashCode() {
            return 31 * appId.hashCode() + busCode.hashCode();
        }

        @Override
        public String toString() {
            return appId + ":" + busCode;
        }
    }

    public static class ParticipantMeta {

        private final SagaTccParticipant participant;
        private final Class<?> requestClass;

        ParticipantMeta(SagaTccParticipant participant, Class<?> requestClass) {
            this.participant = participant;
            this.requestClass = requestClass;
        }

        public SagaTccParticipant getParticipant() {
            return participant;
        }

        public Class<?> getRequestClass() {
            return requestClass;
        }
    }
}
