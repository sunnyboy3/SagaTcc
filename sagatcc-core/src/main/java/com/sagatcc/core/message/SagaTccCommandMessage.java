package com.sagatcc.core.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SagaTccCommandMessage {

    private String messageKey;
    private String sagaId;
    private Long branchId;
    private String coordinatorApp;
    private String targetApp;
    private String busCode;
    private SagaTccAction action;
    /**
     * 生产方 DTO 类名，仅保留用于问题诊断。消费方根据 targetApp + busCode
     * 解析实际请求类型，避免 Java 类改名影响执行中的 Saga 命令。
     */
    private String requestClass;
    private String requestJson;
    private int attempt;

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public Long getBranchId() {
        return branchId;
    }

    public void setBranchId(Long branchId) {
        this.branchId = branchId;
    }

    public String getCoordinatorApp() {
        return coordinatorApp;
    }

    public void setCoordinatorApp(String coordinatorApp) {
        this.coordinatorApp = coordinatorApp;
    }

    public String getTargetApp() {
        return targetApp;
    }

    public void setTargetApp(String targetApp) {
        this.targetApp = targetApp;
    }

    public String getBusCode() {
        return busCode;
    }

    public void setBusCode(String busCode) {
        this.busCode = busCode;
    }

    public SagaTccAction getAction() {
        return action;
    }

    public void setAction(SagaTccAction action) {
        this.action = action;
    }

    public String getRequestClass() {
        return requestClass;
    }

    public void setRequestClass(String requestClass) {
        this.requestClass = requestClass;
    }

    public String getRequestJson() {
        return requestJson;
    }

    public void setRequestJson(String requestJson) {
        this.requestJson = requestJson;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }
}
