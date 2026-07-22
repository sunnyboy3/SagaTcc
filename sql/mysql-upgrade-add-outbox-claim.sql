-- 仅支持离线迁移。执行本文件前必须停止生产方、协调器和消费方，排空
-- command/result Topic 及死信队列，然后完成数据备份。
-- 旧版参与方记录不包含 coordinator_app，因此迁移进行中的 Saga 会有业务重复执行风险。
-- 只有所有协调器事务和分支均已进入终态，且不存在仍可投递的 outbox 命令时，
-- 才允许继续执行任何表结构变更。
drop procedure if exists saga_tcc_assert_offline_upgrade_ready;
delimiter //
create procedure saga_tcc_assert_offline_upgrade_ready()
begin
  declare active_count bigint default 0;

  select count(*) into active_count
  from saga_tcc_transaction
  where status not in ('COMMITTED', 'CANCELLED', 'FAILED');
  if active_count <> 0 then
    signal sqlstate '45000'
      set message_text = 'SagaTcc upgrade aborted: non-terminal transactions exist';
  end if;

  select count(*) into active_count
  from saga_tcc_branch
  where status not in ('CONFIRMED', 'CANCELLED', 'FAILED');
  if active_count <> 0 then
    signal sqlstate '45000'
      set message_text = 'SagaTcc upgrade aborted: non-terminal branches exist';
  end if;

  select count(*) into active_count
  from saga_tcc_outbox
  where status in ('NEW', 'FAILED', 'SENDING');
  if active_count <> 0 then
    signal sqlstate '45000'
      set message_text = 'SagaTcc upgrade aborted: deliverable outbox rows exist';
  end if;
end//
delimiter ;

call saga_tcc_assert_offline_upgrade_ready();
drop procedure saga_tcc_assert_offline_upgrade_ready;

-- 每张表的重建操作有意合并到一条 ALTER 中，以控制离线迁移时间和临时磁盘占用。
-- Java 中的路由键和幂等标识区分大小写，因此固定使用 utf8mb4_bin，
-- 不继承可能随 MySQL 版本变化的默认排序规则。
alter table saga_tcc_transaction
  convert to character set utf8mb4 collate utf8mb4_bin,
  row_format = dynamic,
  modify column next_retry_time datetime(3) not null,
  modify column create_time datetime(3) not null,
  modify column update_time datetime(3) not null;

alter table saga_tcc_branch
  convert to character set utf8mb4 collate utf8mb4_bin,
  row_format = dynamic,
  add column failure_attempt int not null default 0 after cancel_attempts,
  drop index idx_saga_tcc_branch_status_retry,
  add key idx_saga_tcc_branch_status_retry (status, next_retry_time, id),
  modify column next_retry_time datetime(3) not null,
  modify column create_time datetime(3) not null,
  modify column update_time datetime(3) not null;

alter table saga_tcc_outbox
  convert to character set utf8mb4 collate utf8mb4_bin,
  row_format = dynamic,
  add column action varchar(32) null after tag,
  add column command_attempt int not null default 0 after action,
  add column claim_token varchar(64) null after attempts,
  add key idx_saga_tcc_outbox_claim_token (claim_token),
  add key idx_saga_tcc_outbox_branch_attempt
    (branch_id, status, action, command_attempt, attempts),
  modify column next_retry_time datetime(3) not null,
  modify column create_time datetime(3) not null,
  modify column update_time datetime(3) not null;

-- 预检已保证不存在活动分支。这里防御性地废弃旧版 outbox 元数据；
-- 预检成功后，此更新不应影响任何仍可投递的记录。
update saga_tcc_outbox
set status = 'DISCARDED', claim_token = null, update_time = current_timestamp(3)
where action is null and status in ('NEW', 'FAILED', 'SENDING');

alter table saga_tcc_participant_log
  convert to character set utf8mb4 collate utf8mb4_bin,
  row_format = dynamic,
  add column coordinator_app varchar(128) null after local_app,
  modify column create_time datetime(3) not null,
  modify column update_time datetime(3) not null;

update saga_tcc_participant_log
set coordinator_app = '__legacy__'
where coordinator_app is null;

alter table saga_tcc_participant_log
  modify column coordinator_app varchar(128) not null,
  drop index uk_saga_tcc_participant_branch,
  add unique key uk_saga_tcc_participant_branch
    (local_app, coordinator_app, saga_id, branch_id);
