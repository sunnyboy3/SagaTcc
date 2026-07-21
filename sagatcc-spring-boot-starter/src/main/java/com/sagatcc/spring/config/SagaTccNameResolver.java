package com.sagatcc.spring.config;

import com.sagatcc.core.api.SagaTccException;

import org.springframework.core.env.Environment;

public final class SagaTccNameResolver {

    private SagaTccNameResolver() {
    }

    public static String applicationName(SagaTccProperties properties, Environment environment) {
        String configured = properties.getApplicationName();
        if (configured != null && configured.trim().length() > 0) {
            return validateApplicationName(configured.trim());
        }
        String springName = environment.getProperty("spring.application.name");
        if (springName != null && springName.trim().length() > 0) {
            return validateApplicationName(springName.trim());
        }
        throw new SagaTccException("sagatcc.application-name or spring.application.name is required");
    }

    public static String validateApplicationName(String applicationName) {
        if (applicationName == null || applicationName.length() == 0 || applicationName.length() > 128
                || !isRocketMqName(applicationName)) {
            throw new SagaTccException("SagaTcc application name must contain only RocketMQ-safe characters "
                    + "[%|a-zA-Z0-9_-] and not exceed 128 characters");
        }
        return applicationName;
    }

    public static boolean isRocketMqName(String value) {
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (!(current == '%' || current == '|' || current == '_' || current == '-'
                    || current >= 'a' && current <= 'z'
                    || current >= 'A' && current <= 'Z'
                    || current >= '0' && current <= '9')) {
                return false;
            }
        }
        return true;
    }
}
