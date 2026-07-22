# SagaTcc

SagaTcc 是从 EasyTransaction 的 Saga-TCC 思路中独立出来的轻量项目，只保留 Saga-TCC 编排能力，并将服务间通信收敛为 RocketMQ，不依赖 ZooKeeper。

## 目标

- 主控方本地事务内不等待对端 Try 返回，只记录 Saga 分支和 outbox，避免远程慢调用拉长数据库事务持锁时间。
- 使用 RocketMQ 推动 Try / Confirm / Cancel，具备削峰能力。
- 使用本地数据库持久化事务、分支、outbox 和参与方幂等日志，保证进程重启后可恢复。
- 支持多实例水平扩展。消费者组由应用名区分；同一应用多实例共享一个 group；outbox 使用带租约的 CAS 抢占，避免多个实例同时发送同一行。
- 对空回滚、悬挂、重复消息、消息发送失败、Confirm/Cancel 重试等边界做默认保护。

## 模块

| 模块 | 说明 |
| --- | --- |
| `sagatcc-core` | API、注解、消息模型、状态模型 |
| `sagatcc-spring-boot-starter` | Spring Boot 自动配置、JDBC 存储、RocketMQ 发布与消费、恢复任务 |

## 核心流程

1. 主控方在本地事务中调用 `SagaTccOperations.begin(...)`。
2. 主控方通过 `enlist(request)` 登记一个或多个 SagaTcc 分支。
3. 本地事务 `beforeCommit` 阶段写入 `saga_tcc_transaction`、`saga_tcc_branch`、`saga_tcc_outbox`。
4. 本地事务提交后由独立、有界线程池批量抢占并发布 RocketMQ Try；业务线程不等待 Broker，发布失败由 outbox 扫描任务重试。
5. 参与方消费 Try 命令，执行 `sagaTry`，并向协调方 result topic 返回结果。
6. Try 全部成功后，协调方发布 Confirm；任一 Try 失败后，协调方发布 Cancel。
7. Confirm/Cancel 失败会持续重试；超过 `sagatcc.max-attempts` 后事务进入 `FAILED`，由人工或运维任务介入。

## 快速接入

### 1. 建表

在每个参与 SagaTcc 的业务库执行：

```sql
source sql/mysql.sql;
```

协调方需要事务表、分支表和 outbox 表；参与方需要 participant log 表。为了部署简单，SQL 默认放在同一个库里。

如果希望业务表与 SagaTcc 表使用同一 MySQL 实例下的不同 Schema，可以先在目标 Schema
执行 `sql/mysql.sql`，然后配置 `sagatcc.schema`。starter 会继续使用业务 `DataSource`
和同一个本地事务，只把 SagaTcc SQL 路由到限定的 Schema：

```yaml
sagatcc:
  schema: saga_store
```

Schema 名只允许字母、数字和下划线，最长 64 个字符。业务数据库账号必须同时拥有业务
Schema 和 SagaTcc Schema 的访问权限；不要为 SagaTcc Schema 创建第二个 `DataSource`。

### 2. 引入依赖

```xml
<dependency>
    <groupId>com.sagatcc</groupId>
    <artifactId>sagatcc-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 3. 在启动类显式启用

引入 starter 后默认不会注册 SagaTcc Bean、RocketMQ 监听器或后台任务。只有在 Spring Boot
启动类上添加 `@EnableSagaTcc` 才会启用：

```java
import com.sagatcc.spring.annotation.EnableSagaTcc;

@SpringBootApplication
@EnableSagaTcc
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

不需要 SagaTcc 的应用只引入依赖但不添加该注解即可。

### 4. 配置

```yaml
spring:
  application:
    name: order-service
  datasource:
    url: jdbc:mysql://127.0.0.1:3306/order_db?useSSL=false&characterEncoding=utf8
    username: root
    password: root

rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: order-service-sagatcc-producer

sagatcc:
  application-name: ${spring.application.name}
  # schema: saga_store
  transaction-manager-bean-name: transactionManager
  # 未单独配置业务编码时使用的默认模式
  branch-execution-mode: PARALLEL
  # 按 begin 的 businessCode 覆盖默认模式，同一个服务可以混合使用
  branch-execution-modes:
    createOrder: SEQUENTIAL
    batchCreateOrder: PARALLEL
  max-attempts: 16
  retry-base-delay-millis: 1000
  retry-max-delay-millis: 60000
  retry-jitter-percent: 20
  scan-batch-size: 100
  max-branches-per-saga: 1000
  max-request-bytes: 1048576
  max-saga-payload-bytes: 10485760
  max-message-bytes: 4194304
  outbox-claim-batch-size: 20
  outbox-publish-concurrency: 4
  outbox-claim-timeout-millis: 30000
  rocketmq:
    send-timeout-millis: 3000
    command-topic: sagatcc-command
    result-topic: sagatcc-result
```

`branch-execution-mode` 是应用内所有 Saga 的默认调度模式；`branch-execution-modes` 以
`sagaTccOperations.begin(businessCode, businessId)` 的 `businessCode` 为键覆盖默认值。因此同一个
服务可以让 `createOrder` 顺序执行，同时让 `batchCreateOrder` 或其他事务继续并行：

- `PARALLEL`（默认）：Try、Confirm、Cancel 在各自阶段内并行派发，吞吐量更高，行为与旧版本一致。
- `SEQUENTIAL`：协调器只派发一个活动分支；Try、Confirm 严格按照 `enlist` 登记顺序执行，Cancel 按登记顺序逆序补偿。只有当前分支成功后才会创建下一个分支命令，失败重试期间不会越过当前分支。

业务编码按原值精确匹配，未匹配时回退到 `branch-execution-mode`。顺序模式不要求 RocketMQ 提供全局有序消息，协调器根据事务表中已有的 `business_code` 选择模式，并通过分支状态和 outbox 在同一本地事务中推进，因此服务重启和消息重复不会改变分支顺序。若某个 Try 永久失败，尚未执行 Try 的后续分支直接标记为 `CANCELLED`，已执行分支逆序补偿；若 Confirm/Cancel 永久失败，后续未派发分支标记为 `FAILED`，Saga 停止推进并进入 `FAILED`，等待人工处理。运行中不要修改已有业务编码的调度模式，应先等待该业务编码下未完成的 Saga 全部结束。

生产环境、高并发或服务数量较多时，强烈建议使用按应用拆 topic，避免每个应用的 consumer group 都接收共享 topic 的全部消息：

```yaml
sagatcc:
  rocketmq:
    per-application-topic: true
    command-topic-prefix: sagatcc-command-
    result-topic-prefix: sagatcc-result-
    command-topic: sagatcc-command-${spring.application.name}
    result-topic: sagatcc-result-${spring.application.name}
```

### 5. 定义请求

```java
@SagaTccBusiness(appId = "wallet-service", busCode = "walletPay")
public class WalletPayRequest implements SagaTccRequest {
    private Long userId;
    private Long amount;
}
```

### 6. 实现参与方

```java
@Component
public class WalletPayParticipant implements SagaTccParticipant<WalletPayRequest> {

    @Override
    @Transactional
    public void sagaTry(WalletPayRequest request) {
        // freeze balance
    }

    @Override
    @Transactional
    public void sagaConfirm(WalletPayRequest request) {
        // deduct frozen balance
    }

    @Override
    @Transactional
    public void sagaCancel(WalletPayRequest request) {
        // release frozen balance; empty rollback must be safe
    }
}
```

参与方回调由 starter 放在同一个 `REQUIRES_NEW` 本地事务中执行，幂等日志与业务修改必须共同提交。回调可以不写 `@Transactional`，或使用默认 `REQUIRED` 加入该事务；不要使用 `REQUIRES_NEW`、`NOT_SUPPORTED`、异步执行，也不要切换到另一个事务管理器，否则会破坏二者原子性。

### 7. 主控方登记分支

```java
@Transactional
public Long createOrder(Long userId, Long amount) {
    Long orderId = orderRepository.insert(userId, amount);

    sagaTccOperations.begin("createOrder", String.valueOf(orderId));

    WalletPayRequest payRequest = new WalletPayRequest();
    payRequest.setUserId(userId);
    payRequest.setAmount(amount);
    sagaTccOperations.enlist(payRequest);

    return orderId;
}
```

`begin/enlist` 必须在一个可写的 Spring 本地事务内调用。starter 会在调用点直接校验，避免业务数据与 Saga 记录不在同一事务而产生不一致。

`sagatcc.transaction-manager-bean-name` 必须指向管理 SagaTcc `JdbcTemplate` 数据源的业务事务管理器；starter 会把协调器、参与方幂等和恢复任务统一绑定到它，并在 `begin/enlist` 时校验当前事务确实持有该数据源。`REQUIRES_NEW` 会挂起并恢复外层 Saga 上下文。通过 `@Transactional(NESTED)` 暴露的保存点会被 `begin/enlist` 立即拒绝；程序化 NESTED 在 Spring Boot 2.7 中无法被可靠感知，因此同样禁止在该边界内调用 SagaTcc API。

如果不希望业务请求 DTO 依赖 `@SagaTccBusiness`，可以把注解放到参与方，并在登记分支时显式传入路由：

```java
public class WalletPayRequest implements SagaTccRequest {
    private Long userId;
    private Long amount;
}

@Component
@SagaTccBusiness(appId = "wallet-service", busCode = "walletPay")
public class WalletPayParticipant implements SagaTccParticipant<WalletPayRequest> {
    // sagaTry / sagaConfirm / sagaCancel
}

sagaTccOperations.enlist("wallet-service", "walletPay", payRequest);
```

## 扩展点

- `SagaMessagePublisher`：替换消息传输实现。
- `SagaTccRepository`：替换协调方事务、分支和 outbox 存储；自定义实现必须保留状态迁移、attempt、claim token 的 CAS 语义，真正实现 `findTransactionForUpdate` 的同 Saga 串行化，并保证 `findBranches` 按 `branchNo` 升序返回。
- `ParticipantLogRepository`：替换参与方幂等存储。
- JDBC 自定义存储应同时实现 `SagaTccDataSourceProvider`，starter 才能校验配置的事务管理器确实覆盖业务、协调 outbox 和参与方幂等写入；非 JDBC 实现必须自行提供等价原子性保证，starter 无法代为验证。
- `SagaTccFailureClassifier`：区分可重试与永久性参与方异常。并行模式下，永久性 Confirm/Cancel 失败后其余已决分支继续执行；顺序模式下停止后续调度并将未派发分支置为 `FAILED`；父事务最终聚合为 `FAILED`。

starter 使用名为 `sagaTccObjectMapper` 的独立协议 `ObjectMapper`，固定使用 lowerCamelCase 字段并忽略未知字段，避免业务应用自己的命名策略或严格反序列化配置改变 Saga 消息协议。如需注册自定义 DTO 模块，可以声明同名 Bean 覆盖，但必须保持跨服务协议配置一致。

命令通过 `targetApp + busCode` 定位参与方，并使用参与方注册的请求类型反序列化。消息中的 `requestClass` 仅保留作诊断信息，不参与路由或兼容性校验，因此 DTO 改包、改名不会阻断已有 Saga 的 Confirm/Cancel；请求 JSON 本身仍需保持向后兼容。

以上类型都可通过声明同类型 Spring Bean 覆盖默认实现。

## 边界设计

- 本地事务不等对端：主控方只写库和 outbox；RocketMQ Try 由提交后的调度线程发送。
- 消息发送失败：outbox 定时批量抢占；先无锁筛选候选主键，再按主键执行短 CAS 更新，数据库死锁或锁获取失败会短暂退避重试；发送前使用 claim token 和租约，只有 RocketMQ 返回 `SEND_OK` 才标记 `SENT`。
- 陈旧消息：阶段/attempt 切换时旧记录进入 `DISCARDED`；投递次数耗尽进入 `DEAD`，不会长期污染 ready 索引。
- 消息重复：`saga_tcc_participant_log` 按 `local_app + coordinator_app + saga_id + branch_id` 幂等。
- 标识一致性：四张表固定使用 `utf8mb4_bin`，Saga、应用和业务路由键与 Java 一样区分大小写，避免 MySQL 5.7/8 默认排序规则差异改变幂等语义。
- 空回滚：参与方收到 Cancel 且 Try 未成功时，直接记录 Cancel 成功。
- 悬挂：Cancel 成功后再收到 Try，Try 直接跳过。
- Confirm 前置保护：Confirm 到达但 Try 未成功时返回失败，协调方继续重试或进入失败态。
- 对端耗时长：只占用参与方本地事务资源，不阻塞主控方数据库事务。
- 多实例扩展：同应用实例使用同一 RocketMQ consumer group；不同应用建议使用不同 group。
- 跨服务流量大：开启 `per-application-topic`，避免所有服务都收到共享 command topic 的广播式流量。
- 长时间失败：超过最大重试次数标记 `FAILED`，保留库内状态用于人工恢复。
- 已决阶段失败：并行模式继续执行其余分支并在全部分支终态后聚合为 `FAILED`；顺序模式停止后续分支，避免越过失败节点继续产生业务副作用。
- 可重试 Try 失败：保持 `TRYING` 并退避重试；永久性 Try 失败才进入 Cancel。退避带随机抖动，避免故障恢复时惊群。
- 迟到/重复结果：结果只有在 Saga、分支路由和当前状态都匹配时才以 CAS 方式迁移，防止迟到的 Try 结果把 Confirm/Cancel 状态回退。
- 请求串改：参与方记录并校验 SHA-256 请求摘要，同一 Saga 分支出现不同路由或载荷时拒绝执行。
- 时钟一致性：租约和重试截止时间均由 MySQL `current_timestamp(3)` 计算，避免应用节点与数据库时钟偏差。
- 载荷保护：同时限制单请求、单 Saga 总载荷和最终 RocketMQ 消息 UTF-8 字节数；`max-message-bytes` 必须不大于 Broker 的 `maxMessageSize`。

## 数据库升级

新建库直接执行 `sql/mysql.sql`。已有旧表需要执行：

```sql
source sql/mysql-upgrade-add-outbox-claim.sql;
```

该脚本是停机迁移，不支持新旧版本滚动混跑。执行前必须停止 Saga 生产、协调器和消费者，等待所有 transaction/branch 进入终态，确认不存在 `NEW/FAILED/SENDING` outbox，排空 command/result topic 及 DLQ，并备份数据库。脚本会在任何 DDL 前重复检查这些条件，存在未完成数据时用 `SIGNAL` 直接中止。迁移账号需要 `CREATE/DROP ROUTINE` 以及表 DDL 权限；四张大表会离线重建，应提前按数据量预估停机时间和至少一份表重建的临时磁盘空间。旧 participant log 缺少 `coordinator_app`，无法安全迁移进行中的 Saga；迁移后也禁止重放迁移前的命令或 DLQ。

## 生产上线检查

- 当前自动化测试覆盖 H2/MySQL 兼容 SQL、64 路参与方重复投递、12 路/96 条 outbox 抢占、24 分支最后结果并发、乱序/重复 attempt、最大重试和 UTF-8 字节边界。上线前仍应在目标 MySQL 5.7/8 与 RocketMQ 集群执行压测和故障注入，H2 不能替代 InnoDB 锁与执行计划验证。
- MySQL 必须启用 `STRICT_TRANS_TABLES` 或 `STRICT_ALL_TABLES`，确保超长字段和约束错误直接失败；非严格模式可能把数据截断为警告，破坏路由与幂等标识。
- 连接池至少覆盖业务并发、`outbox-publish-concurrency` 和一个 recovery worker；多实例扩容时同步观察数据库锁等待、死锁、oldest outbox age、`DEAD` 数量和各状态积压。
- 表不会自动删除历史记录。按业务审计要求分批归档终态 transaction/branch 及 `SENT/DEAD/DISCARDED` outbox；participant log 的保留期必须长于 outbox 重试、RocketMQ 重试和 DLQ 可重放窗口，否则历史消息可能再次执行业务。
- 大流量必须开启按应用 topic，并根据 Broker 延迟、数据库容量逐步调高发布并发；不要直接把扫描批次或线程数调到上限。

## 与原 EasyTransaction SagaTcc 的差异

- 原实现使用 RPC 调用 `sagaTry / sagaConfirm / sagaCancel`；本项目只使用 RocketMQ。
- 原项目包含 TCC、补偿、可靠消息、ZK 主从选举、ZK 字符串编码等能力；本项目全部移除。
- 本项目使用数据库 outbox 作为可靠消息边界，不依赖 ZK。
- 本项目把参与方幂等日志独立成表，明确处理重复、空回滚和悬挂。

## 编译

```bash
mvn clean test
```
## MQ发送消息实例
```sql
sagatcc-command-order-service
{"messageKey":"d1cd203b9c1440d0a8aa1baf2d0e9f43-2-TRY-5","sagaId":"d1cd203b9c1440d0a8aa1baf2d0e9f43","branchId":2,"coordinatorApp":"order-service","targetApp":"order-service","busCode":"orderFinalize","action":"TRY","requestClass":"com.sagatcc.demo.order.OrderFinalizeRequest","requestJson":"{\"orderId\":1}","attempt":5}

sagatcc-command-wallet-service
{"messageKey":"d1cd203b9c1440d0a8aa1baf2d0e9f43-1-TRY-4","sagaId":"d1cd203b9c1440d0a8aa1baf2d0e9f43","branchId":1,"coordinatorApp":"order-service","targetApp":"wallet-service","busCode":"walletPay","action":"TRY","requestClass":"com.sagatcc.demo.wallet.api.WalletPayRequest","requestJson":"{\"userId\":1,\"amount\":100}","attempt":4}
```
