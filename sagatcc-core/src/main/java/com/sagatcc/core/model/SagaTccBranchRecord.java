package com.sagatcc.core.model;

import java.util.Date;

public class SagaTccBranchRecord {

    private Long id;
    private String sagaId;
    private int branchNo;
    private String targetApp;
    private String busCode;
    private String requestClass;
    private String requestJson;
    private SagaTccBranchStatus status;
    private int tryAttempts;
    private int confirmAttempts;
    private int cancelAttempts;
    private int failureAttempt;
    private String lastError;
    private Date nextRetryTime;
    private Date createTime;
    private Date updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public int getBranchNo() {
        return branchNo;
    }

    public void setBranchNo(int branchNo) {
        this.branchNo = branchNo;
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

    public SagaTccBranchStatus getStatus() {
        return status;
    }

    public void setStatus(SagaTccBranchStatus status) {
        this.status = status;
    }

    public int getTryAttempts() {
        return tryAttempts;
    }

    public void setTryAttempts(int tryAttempts) {
        this.tryAttempts = tryAttempts;
    }

    public int getConfirmAttempts() {
        return confirmAttempts;
    }

    public void setConfirmAttempts(int confirmAttempts) {
        this.confirmAttempts = confirmAttempts;
    }

    public int getCancelAttempts() {
        return cancelAttempts;
    }

    public void setCancelAttempts(int cancelAttempts) {
        this.cancelAttempts = cancelAttempts;
    }

    public int getFailureAttempt() {
        return failureAttempt;
    }

    public void setFailureAttempt(int failureAttempt) {
        this.failureAttempt = failureAttempt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Date getNextRetryTime() {
        return nextRetryTime;
    }

    public void setNextRetryTime(Date nextRetryTime) {
        this.nextRetryTime = nextRetryTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
