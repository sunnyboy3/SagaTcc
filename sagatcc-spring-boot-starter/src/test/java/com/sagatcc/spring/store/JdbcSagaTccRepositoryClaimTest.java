package com.sagatcc.spring.store;

import java.util.Collections;

import com.sagatcc.spring.config.SagaTccProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcSagaTccRepositoryClaimTest {

    @Test
    void claimUsesReadOnlyCandidateSelectionAndRetriesShortPrimaryKeyCasAfterDeadlock() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        SagaTccProperties properties = new SagaTccProperties();
        properties.setOutboxClaimBatchSize(1);
        JdbcSagaTccRepository repository = new JdbcSagaTccRepository(jdbcTemplate, properties);

        when(jdbcTemplate.queryForList(anyString(), eq(Long.class), any(), any(), any()))
                .thenReturn(Collections.singletonList(7L));
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenThrow(new PessimisticLockingFailureException("模拟数据库死锁"))
                .thenReturn(1);

        repository.claimReadyOutbox(1);

        ArgumentCaptor<String> selectionSql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(2)).queryForList(
                selectionSql.capture(), eq(Long.class), any(), any(), any());
        assertThat(selectionSql.getAllValues())
                .allSatisfy(sql -> assertThat(sql.toLowerCase())
                        .contains("select o.id", "join saga_tcc_branch", "join saga_tcc_transaction"));

        ArgumentCaptor<String> claimSql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(2)).update(
                claimSql.capture(), any(), any(), any(), any(), any(), any());
        assertThat(claimSql.getAllValues()).allSatisfy(sql -> {
            assertThat(sql.toLowerCase()).contains("id in (?)", "order by id");
            assertThat(sql.toLowerCase()).doesNotContain("exists", "saga_tcc_branch", "saga_tcc_transaction");
        });
    }
}
