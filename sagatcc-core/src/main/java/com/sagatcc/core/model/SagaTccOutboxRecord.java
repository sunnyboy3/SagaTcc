package com.sagatcc.core.model;

import java.util.Date;

import com.sagatcc.core.message.SagaTccAction;

public class SagaTccOutboxRecord {

    private Long id;
    private String messageKey;
    private String sagaId;
    private Long branchId;
    private String topic;
    private String tag;
    private String payload;
    private SagaTccOutboxStatus status;
    private int attempts;
    private SagaTccAction action;
    private int commandAttempt;
    private String claimToken;
    private Date nextRetryTime;
    private Date createTime;
    private Date updateTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public SagaTccOutboxStatus getStatus() {
        return status;
    }

    public void setStatus(SagaTccOutboxStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public SagaTccAction getAction() {
        return action;
    }

    public void setAction(SagaTccAction action) {
        this.action = action;
    }

    public int getCommandAttempt() {
        return commandAttempt;
    }

    public void setCommandAttempt(int commandAttempt) {
        this.commandAttempt = commandAttempt;
    }

    public String getClaimToken() {
        return claimToken;
    }

    public void setClaimToken(String claimToken) {
        this.claimToken = claimToken;
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
