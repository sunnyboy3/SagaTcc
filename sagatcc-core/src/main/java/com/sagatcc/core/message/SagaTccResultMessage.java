package com.sagatcc.core.message;

public class SagaTccResultMessage {

    private String messageKey;
    private String sagaId;
    private Long branchId;
    private String coordinatorApp;
    private String targetApp;
    private String busCode;
    private SagaTccAction action;
    private int attempt;
    private Boolean success;
    private Boolean retryable;
    private String errorCode;
    private String errorMessage;

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

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public boolean isSuccess() {
        return Boolean.TRUE.equals(success);
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public boolean isRetryable() {
        return Boolean.TRUE.equals(retryable);
    }

    public void setRetryable(Boolean retryable) {
        this.retryable = retryable;
    }

    public boolean successSpecified() {
        return success != null;
    }

    public boolean retryableSpecified() {
        return retryable != null;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
