-- OFFLINE migration only. Stop producers, coordinators and consumers, drain
-- command/result topics and DLQs, then take a backup before running this file.
-- Legacy participant rows do not record coordinator_app, so an in-flight Saga
-- cannot be migrated without risking a second business execution. Abort before
-- any schema change unless every coordinator and branch is already terminal and
-- no outbox command can still be delivered.
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

-- Each table's rebuilding changes are deliberately combined into one ALTER to
-- bound offline migration time and temporary disk usage. Routing keys and
-- idempotency identities are case-sensitive in Java, so pin utf8mb4_bin rather
-- than inheriting version-dependent MySQL defaults.
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

-- The preflight guarantees there are no active branches. Retire any legacy
-- outbox metadata defensively; this update must therefore affect no deliverable
-- row after a successful preflight.
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
