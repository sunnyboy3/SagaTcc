package com.sagatcc.spring.store;

import javax.sql.DataSource;

/**
 * Optional contract for JDBC-backed persistence extensions. Exposing the
 * enlisted DataSource lets SagaTcc fail fast when the configured transaction
 * manager does not cover business, coordinator, and idempotency writes.
 */
public interface SagaTccDataSourceProvider {

    DataSource sagaTccDataSource();
}
