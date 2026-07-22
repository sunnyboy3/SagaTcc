package com.sagatcc.spring.store;

import javax.sql.DataSource;

/**
 * JDBC 持久化扩展可选实现的契约。通过暴露已加入事务的 DataSource，
 * 当配置的事务管理器未覆盖业务、协调器和幂等写入时，SagaTcc 可以快速失败。
 */
public interface SagaTccDataSourceProvider {

    DataSource sagaTccDataSource();
}
