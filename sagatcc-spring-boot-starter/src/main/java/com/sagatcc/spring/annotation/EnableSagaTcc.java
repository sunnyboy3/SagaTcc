package com.sagatcc.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * 显式启用 SagaTcc 的 Spring Boot 配置、消息监听器和后台任务。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(EnableSagaTcc.Marker.class)
public @interface EnableSagaTcc {

    /** 供自动配置识别启用状态的内部标记。 */
    final class Marker {

        public Marker() {
        }
    }
}

