package com.sagatcc.spring.config;

/**
 * Saga 内部分支的阶段调度方式。
 */
public enum SagaTccBranchExecutionMode {

    /** 所有分支在同一事务阶段内并行调度。 */
    PARALLEL,

    /** Try、Confirm 按登记顺序执行，Cancel 按登记顺序逆序补偿。 */
    SEQUENTIAL
}
