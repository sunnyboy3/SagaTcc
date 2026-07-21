package com.sagatcc.core.model;

import java.util.Date;

public class SagaTccTransactionRecord {

    private String sagaId;
    private String coordinatorApp;
    private String businessCode;
    private String businessId;
    private SagaTccTransactionStatus status;
    private int branchCount;
    private String lastError;
    private Date nextRetryTime;
    private Date createTime;
    private Date updateTime;

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public String getCoordinatorApp() {
        return coordinatorApp;
    }

    public void setCoordinatorApp(String coordinatorApp) {
        this.coordinatorApp = coordinatorApp;
    }

    public String getBusinessCode() {
        return businessCode;
    }

    public void setBusinessCode(String businessCode) {
        this.businessCode = businessCode;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public SagaTccTransactionStatus getStatus() {
        return status;
    }

    public void setStatus(SagaTccTransactionStatus status) {
        this.status = status;
    }

    public int getBranchCount() {
        return branchCount;
    }

    public void setBranchCount(int branchCount) {
        this.branchCount = branchCount;
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
