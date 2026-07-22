package com.sagatcc.spring.store;

import java.util.Arrays;

import com.sagatcc.core.model.SagaTccBranchStatus;
import com.sagatcc.spring.config.SagaTccProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

class JdbcSagaTccRepositoryDiscardTest {

    @Test
    void terminalBranchDiscardsOutboxByOrderedPrimaryKeyUpdates() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JdbcSagaTccRepository repository = new JdbcSagaTccRepository(
                jdbcTemplate, new SagaTccProperties());

        when(jdbcTemplate.update(anyString(), any(), any(), any(), any())).thenReturn(1);
        when(jdbcTemplate.queryForList(anyString(), eq(Long.class), eq(41L)))
                .thenReturn(Arrays.asList(7L, 12L));

        boolean transitioned = repository.transitionBranchStatus(
                41L, SagaTccBranchStatus.TRYING, SagaTccBranchStatus.TRY_SUCCEEDED, "成功");

        assertThat(transitioned).isTrue();
        ArgumentCaptor<String> selectionSql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).queryForList(selectionSql.capture(), eq(Long.class), eq(41L));
        assertThat(selectionSql.getValue().toLowerCase())
                .contains("select id", "branch_id = ?", "order by id")
                .doesNotContain("for update");

        ArgumentCaptor<Object> outboxId = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate, times(2)).update(
                argThat(sql -> sql.toLowerCase().contains("set status = 'discarded'")
                        && sql.toLowerCase().contains("where id = ? and status")),
                outboxId.capture());
        assertThat(outboxId.getAllValues()).containsExactly(7L, 12L);
    }
}
