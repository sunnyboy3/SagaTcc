package com.sagatcc.core.context;

public final class SagaTccContextHolder {

    private static final ThreadLocal<SagaTccContext> HOLDER = new ThreadLocal<SagaTccContext>();

    private SagaTccContextHolder() {
    }

    public static SagaTccContext get() {
        return HOLDER.get();
    }

    public static void set(SagaTccContext context) {
        HOLDER.set(context);
    }

    public static void clear() {
        HOLDER.remove();
    }
}
