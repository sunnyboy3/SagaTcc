package com.sagatcc.core.context;

import com.sagatcc.core.api.SagaTccRequest;

public final class SagaTccEnlistment {

    private final String targetApplication;
    private final String businessCode;
    private final SagaTccRequest request;

    public SagaTccEnlistment(String targetApplication, String businessCode, SagaTccRequest request) {
        this.targetApplication = targetApplication;
        this.businessCode = businessCode;
        this.request = request;
    }

    public String getTargetApplication() {
        return targetApplication;
    }

    public String getBusinessCode() {
        return businessCode;
    }

    public SagaTccRequest getRequest() {
        return request;
    }
}
