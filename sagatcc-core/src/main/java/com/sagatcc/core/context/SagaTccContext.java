package com.sagatcc.core.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.sagatcc.core.api.SagaTccBusiness;
import com.sagatcc.core.api.SagaTccRequest;

public class SagaTccContext {

    private final String sagaId;
    private final String coordinatorApp;
    private final String businessCode;
    private final String businessId;
    private final List<SagaTccEnlistment> enlistments = new ArrayList<SagaTccEnlistment>();
    private boolean persisted;

    public SagaTccContext(String sagaId, String coordinatorApp, String businessCode, String businessId) {
        this.sagaId = sagaId;
        this.coordinatorApp = coordinatorApp;
        this.businessCode = businessCode;
        this.businessId = businessId;
    }

    public String getSagaId() {
        return sagaId;
    }

    public String getCoordinatorApp() {
        return coordinatorApp;
    }

    public String getBusinessCode() {
        return businessCode;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void addEnlistment(SagaTccEnlistment enlistment) {
        enlistments.add(enlistment);
    }

    public List<SagaTccEnlistment> getEnlistments() {
        return Collections.unmodifiableList(enlistments);
    }

    /** @deprecated use {@link #addEnlistment(SagaTccEnlistment)} */
    @Deprecated
    public void addRequest(SagaTccRequest request) {
        SagaTccBusiness business = request.getClass().getAnnotation(SagaTccBusiness.class);
        if (business == null) {
            throw new IllegalArgumentException("SagaTcc request missing @SagaTccBusiness: " + request.getClass().getName());
        }
        addEnlistment(new SagaTccEnlistment(business.appId(), business.busCode(), request));
    }

    /** @deprecated use {@link #getEnlistments()} */
    @Deprecated
    public List<SagaTccRequest> getRequests() {
        List<SagaTccRequest> requests = new ArrayList<SagaTccRequest>(enlistments.size());
        for (SagaTccEnlistment enlistment : enlistments) {
            requests.add(enlistment.getRequest());
        }
        return Collections.unmodifiableList(requests);
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }
}
