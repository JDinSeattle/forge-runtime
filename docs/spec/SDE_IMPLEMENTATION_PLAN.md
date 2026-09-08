本方案用于实现 Forge Runtime：面向 Backend / Platform SDE 求职的多模型 Coding Agent 执行平台。

调研日期：2026-09-07。本文是供开发 Agent 使用的实现规格，不包含工期安排。架构、参数和验收指标是本项目的设计选择，尚非实测结果。它与 AGENT_IMPLEMENTATION_PLAN.md 描述同一产品场景的两条实现路线，不能将两份文档中的调度器和任务状态管理直接叠加。

1. 固定产品行为和 SDE 版本的技术目标。

    用户在项目内提交“修复代码、补充功能、运行验证”任务。平台创建隔离工作区，调用模型执行工具循环，交付 patch、验证报告、运行事件和费用记录。用户可以断线重连、补充指令、审批、取消和恢复任务。

    系统必须演示多个 worker 竞争领取任务、worker 进程崩溃后恢复、慢客户端不拖垮执行、跨用户授权边界，以及模型限流时的有界退避。

    第一版运行在 Linux，使用一个 PostgreSQL、多个 Go worker 和一个独立 runner。支持多个受控用户/tenant；任务容器只运行在受控部署内，不开放为匿名任意代码执行服务。Python 作为首个被修复的仓库语言，Go 仓库作为第二个 RepoProfile。

    本版本的主要工程贡献是：

    - 事务化任务提交、幂等 API 和明确的状态迁移。
    - 有公平性与容量控制的任务分发、租约和 fencing。
    - 可查询执行结果的独立 runner 与不确定副作用核对。
    - 持久化事件、SSE 续接、背压与资源上限。
    - 多模型流式适配、共享额度预留和错误恢复。
    - 故障注入、并发测试、压测和可复现实验。

    Agent 必须真正完成小规模代码任务。模型效果通过固定任务 smoke eval 保底；主要验收放在执行平台的正确性和运行特性。

2. 根据当前官方资料确定架构方向。

    下表是对当前文档的工程归纳，不代表这些技术都在 2026 年首次出现，也不代表已证明某种行业采用率。

    | 当前可观察的方向 | 官方依据 | 本项目采取的实现 |
    | --- | --- | --- |
    | Agent 核心服务可被多个客户端使用 | OpenCode 提供独立 HTTP server 与 OpenAPI | CLI、API、worker 分开；客户端不拥有任务生命周期 |
    | 长任务需要持久化执行状态 | Temporal 区分 workflow 历史与可重试 Activity | 实现范围受限的 Go 状态机、步骤账本和恢复协议 |
    | 编辑器接入与工具接入有各自协议 | ACP 连接编辑器与 coding agent；MCP 连接模型应用与工具 | 核心 API 稳定后分别提供 ACP adapter 和 MCP client |
    | 不可信代码执行需要单独隔离层 | Docker rootless 与 gVisor 有明确不同的隔离机制 | runner 独立部署，默认 rootless Docker，保留 gVisor backend |
    | 原生模型能力超出统一文本接口 | 模型 API 包含工具、续接、压缩与异步能力 | 能力注册表、原生 payload 保留、能力组合校验 |
    | AI 可观测规范仍在演进 | OpenTelemetry GenAI 文档已迁到专用仓库 | 使用 OTel trace，固定语义约定版本，业务账本独立于 tracing |
    | 语言运行时提供更可靠的文件访问原语 | Go 提供 traversal-resistant os.Root API | 路径访问使用 os.Root/等价受约束目录 API，并单独做沙箱隔离 |

    依据：[OpenCode Server](https://opencode.ai/docs/server/)、[Temporal Activities](https://docs.temporal.io/activity-definition)、[ACP](https://agentclientprotocol.com/get-started/introduction)、[Docker rootless](https://docs.docker.com/engine/security/rootless/)、[gVisor](https://gvisor.dev/docs/architecture_guide/intro/)、[OTel GenAI](https://github.com/open-telemetry/semantic-conventions-genai)、[Go os.Root](https://go.dev/blog/osroot)。模型与 MCP 的具体依据放在对应实现条目旁。

3. 采用 Go 主实现，并控制基础设施数量。

    | 部件 | 选型 | 用途 |
    | --- | --- | --- |
    | 语言与构建 | Go，固定受支持工具链、go.mod/go.sum | API、worker、runner、CLI 共用领域类型 |
    | 公共接口 | net/http + chi，OpenAPI 3.1，oapi-codegen | HTTP 路由、请求/响应契约和客户端生成 |
    | 数据访问 | pgx + sqlc + goose | 参数化 SQL、显式事务、类型化查询与迁移 |
    | 主数据库 | PostgreSQL | 任务、状态、事件、审批、租约、额度与业务账本 |
    | Agent runtime | 自有的小型显式状态机 | 单 run 的步骤推进与恢复，不实现通用 DAG 平台 |
    | 模型适配 | 厂商 Go SDK；必要时小范围类型化 HTTP adapter | OpenAI Responses、Anthropic Messages |
    | 内部执行协议 | gRPC + Protobuf，远程连接使用 mTLS | worker 与 runner 的操作、检查、取消和快照 |
    | runner 本地账本 | SQLite WAL，synchronous=FULL | 命令进程与本地工件的持久化核对记录 |
    | 沙箱 | rootless Docker；可选 gVisor | 独立工作区、进程与资源边界 |
    | 工件存储 | ArtifactStore，先本地持久化卷 | patch、日志、快照、模型原始响应和验证报告 |
    | CLI | Cobra，先文本流展示 | 提交、观察、审批、恢复和下载工件 |
    | 可观测性 | slog + OpenTelemetry + Prometheus | 日志、trace、指标；本地 compose 提供 collector |
    | 验证与性能 | go test、race detector、fuzz、k6 | 协议/并发/故障验证，HTTP 和 SSE 压测 |

    Python 仅用于需要复用 Python 仓库测试生态的评测脚本；运行时不依赖 Python Agent 服务。Rust 和 C++ 不作为展示语言数量的必选项。后续优化必须先给出 profiling 证据和替换边界。

    主实现只使用 PostgreSQL 领取任务。Redis、Kafka、Temporal 不进入这一条实现路线的默认依赖。将来采用 Temporal 时，应替换 run 调度与恢复层，保留领域 API 和 runner 幂等协议；不要维持两个权威任务状态机。

    版本兼容基线：已查阅的 oapi-codegen v2.8.0 发布说明提供初步 OpenAPI 3.1 支持，要求 Go >=1.25 以及 runtime >=1.6.0。实现时选择仍受支持的 Go 工具链，锁定生成器/运行库版本，并以本项目 schema 做生成与编译验证。OpenAPI 的 security 声明不替代应用授权代码。[oapi-codegen v2.8.0](https://github.com/oapi-codegen/oapi-codegen/releases/tag/v2.8.0)

    选择小型自有 runtime 是本 SDE 项目的训练目标：实现边界限定于一种 coding run。生产选型对照应记录 Temporal 的收益与运维成本；它的 Activity 仍可能重试，因此也不能替应用消除外部副作用的不确定性。[Temporal Activity Idempotency](https://docs.temporal.io/activity-definition)

4. 实现以下拓扑和服务边界。

    ```mermaid
    flowchart TD
        C[CLI / 后续 Web / ACP] --> A[forge-api：身份、任务、事件]
        A --> P[(PostgreSQL)]
        W1[forge-worker 1] <--> P
        W2[forge-worker 2] <--> P
        W1 --> L[模型 Provider]
        W2 --> L
        W1 --> R[forge-runner：操作账本与工作区]
        W2 --> R
        R --> J[(本地 SQLite 操作账本)]
        R --> S[隔离任务容器]
        R --> F[工件与工作区持久化卷]
        A --> F
        E[独立 evaluator] --> A
        E --> V[独立验证容器]
    ```

    forge-api 不执行模型请求或 shell。forge-worker 持有模型凭据、推进状态机，通过内部协议请求执行；forge-runner 控制沙箱和本地操作，不持有模型凭据。任务容器不持有内部 RPC 凭据、数据库凭据或 Docker socket。

    第一版 API、worker 和 runner 可部署在同一主机的独立进程中；端到端测试必须启动至少两个 worker。runner 的工作区按 runner_id 固定归属。worker 故障可由另一个 worker 接管；runner 不可达时不得直接在别处重跑未知命令。

    完整宿主故障后的跨机器恢复列为扩展，前置条件是可信的工作区快照和旧执行已停止的证据。基线不能宣称任意 host 故障都可无损自动恢复。

5. 按领域组织仓库。

    ```text
    cmd/
      forge-api/
      forge-worker/
      forge-runner/
      forge/
    internal/
      domain/             # ID、状态、事件、错误、金额和配置
      httpapi/            # HTTP handler、auth、SSE、错误映射
      application/        # SubmitRun、Approve、Cancel、Resume
      scheduler/          # tenant 轮转、领取、容量、租约
      runtime/            # reducer、driver、step、context、stop
      provider/           # capabilities、openai、anthropic、fake
      quota/              # 并发令牌、token/费用预留和对账
      tool/               # schema、policy、工具调用契约
      runnerclient/       # gRPC client、能力令牌、重连
      runner/             # RPC server、journal、jobs、fencing
      sandbox/            # Docker backend、workspace、snapshot
      artifact/           # store、校验、发布、所有权
      persistence/        # sqlc 查询、事务封装、迁移入口
      eventstream/        # 分页读取、游标、fanout、背压
      telemetry/
    api/openapi.yaml
    proto/runner/v1/runner.proto
    db/migrations/
    db/queries/
    configs/              # 模型、策略、仓库 profile 和预算
    testdata/             # 固定模型响应与小型目标仓库
    tests/integration/
    tests/faults/
    benchmarks/           # k6、工作负载配置与结果
    evals/                # 公开任务描述；隐藏 grader 单独挂载
    deploy/compose/
    docs/adr/
    ```

    依赖方向为 transport → application → domain/interfaces。SQL、SDK、gRPC 与 Docker 属于 adapter。runtime 不依赖 HTTP handler，runner 不依赖模型 SDK。生成代码不承载手写业务逻辑。

6. 定义权威状态与业务表。

    | 表 | 关键字段/约束 |
    | --- | --- |
    | tenants / memberships | tenant_id、principal_id、role；身份来自认证结果 |
    | api_tokens | token_hash、principal_id、expires_at、revoked_at；仅保存不可逆 hash |
    | projects | tenant_id、id、allowed_repo_source、repo_profile_id |
    | runs | tenant_id、id、project_id、parent_run_id、base_commit、input_snapshot、state、version、lease_epoch、lease_owner、lease_until、runner_id、workspace_id、config_snapshot |
    | run_snapshots | run_id、step_seq、state_version、state_json、transcript_refs、workspace_revision；恢复使用已提交版本 |
    | run_messages | run_id、seq、text、applied_step；补充指令追加写入 |
    | steps | run_id、seq、kind、status、input_hash、output_ref；唯一键 run_id+seq |
    | model_attempts | step_id、attempt、provider、model_id、status、raw_ref、usage、request_id、price_version |
    | effects | run_id、step_id、ordinal、operation_id、args_hash、expected_revision、status、receipt_ref；operation_id 唯一 |
    | approvals | effect_id、args_hash、workspace_revision、policy_version、decision、version |
    | run_events | run_id、seq、type、attempt_id、payload、schema_version；唯一键 run_id+seq |
    | idempotency_keys | tenant_id、principal_id、route、key、request_hash、resource_id、response；唯一组合键 |
    | tenant_runtime | tenant_id、max_active、active_count、last_dispatched_at；调度控制行 |
    | provider_quotas / reservations | 共享凭据组、窗口、额度、调用预留、占用、费用状态 |
    | runner_allocations | runner_id、run_id、slots、memory_budget、state；限制实际执行容量 |
    | artifacts | tenant_id、run_id、kind、object_key、sha256、byte_size、state |

    金额使用 int64 的约定最小单位或 NUMERIC，不使用 float。所有业务对象按 tenant_id 建立关联，外键包含 tenant_id，避免合法 ID 被跨 tenant 引用。

    初始 RunState：queued、running、waiting_approval、cancel_requested、cancelled、completed、failed、budget_exhausted、needs_reconciliation。verification_status 单独记录 verified、regression_only、unverified。

    定义以下不变量，并据此写测试：

    - 同一 idempotency key 和相同请求只能对应一个逻辑 run；不同请求 hash 返回冲突。
    - 一个 run 同时只有一个有效推进者；过期 epoch 不能更新状态或启动新写操作。
    - 一个 workspace 同一时间最多有一个写操作；取得数据库租约不等于旧写操作已经停止。
    - 每个已完成步骤引用持久化结果；partial response 不能触发工具执行。
    - 终态不会被迟到响应改回 running；重试整项任务创建新 run 并记录 parent_run_id。
    - cancelled 表示执行器确认停止；仅发出取消请求不能直接产生该状态。
    - 模型声明完成后必须经过可信验证，才能输出 verified。

7. 实现事务化提交与幂等 API。

    POST /v1/projects/{project_id}/runs 接收 task、base_commit、config_id、budget 和 Idempotency-Key。先认证和校验项目归属，再规范化请求并计算 hash。

    单事务完成：校验/写入 idempotency_keys、创建 queued run、创建初始 snapshot、追加 run.created 事件。返回 202 + run_id + Location。响应丢失后，客户端重试获得同一结果。

    并发相同 key 通过唯一约束与冲突查询收敛；不能使用“先 SELECT 再 INSERT”的无锁判断。幂等记录的保留期限写入 API 契约；记录过期后的语义必须明确，不能继续宣称跨无限时间去重。

    事务提交之后 best-effort 发送 PostgreSQL NOTIFY，payload 只包含唤醒提示或对象 ID；worker 和 SSE 服务也定期补读数据库。通知丢失只影响延迟，不影响正确性。NOTIFY 的交付与事务有关，且不能取代持久化事件表。[PostgreSQL NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)

    调用数据库、模型、runner 和工件存储分别设置 context deadline。不要跨模型请求或容器执行持有 SQL 行锁/事务。

8. 实现容量约束下的调度与租约。

    调度采用 tenant 间轮转、tenant 内 FIFO，保留有限的优先级字段。具体实现为：在有可执行队列且未达 active 上限的 tenant_runtime 行中，按 last_dispatched_at 排序，使用 FOR UPDATE SKIP LOCKED 选择一个 tenant，再领取其最早的可运行 run。

    同一事务更新 active_count、run 的 owner/epoch/lease、runner capacity reservation 和调度时间。所有会同时修改 tenant/run 的事务按 tenant_runtime → run → step/effect → quota 的固定顺序取锁；额度模块的独立事务不得反向锁 run。

    SKIP LOCKED 适合多消费者队列表，但不提供通用一致快照或天然公平性；公平策略由上述 tenant 选择与容量约束实现。[PostgreSQL SELECT](https://www.postgresql.org/docs/current/sql-select.html)

    初始运行参数：lease=30s、heartbeat=10s、每 tenant 最多 2 个 active run。所有租约比较使用数据库时间。这些是可调工程参数，不是性能结论。

    worker 仅在本地还有有界执行槽时领取任务；runner 也做实际容量检查。容量拒绝时事务化释放 allocation 并退回队列，附带 not_before，避免热循环。等待审批不占活动执行槽，但保留工作区存储配额。

    更新状态时要求 run_id、lease_owner、lease_epoch、version 匹配，并检查 lease 未失效。影响行数为 0 表示失去推进权，worker 必须停止提交新动作。

    Reconciler 根据旧 epoch 的活动记录恢复容量，不简单依赖计数器自增/自减。active_count 的变化与 run 状态在同一事务；重复完成、重复取消和重复恢复不重复释放槽。

    租约过期的 run 先进入恢复检查。已有已确认结果则复用；存在未决 effect 则联系原 runner。轮转公平性限定为调度机会上的公平，不对非抢占的长任务承诺固定等待上限。

9. 实现范围受限、可恢复的 Go Agent 状态机。

    ```text
    queued -> initialize -> build_context -> model_step
      -> validate_tool_calls -> approval_gate -> execute_effects
      -> ingest_results -> stop_check -> build_context
      -> finish_candidate -> verify -> finalize

    旁路状态：waiting_approval / cancel_requested /
              budget_exhausted / needs_reconciliation / failed
    ```

    reducer 使用显式 State、Event、Command 类型，执行 Transition(state,event) → nextState + commands。时间、随机 ID 与外部结果由事件输入；纯 reducer 不访问网络、数据库和文件系统。

    driver 负责执行 command 并把结果写回。每个完整步骤的输出引用、状态 snapshot 和对应业务事件在同一 PostgreSQL 事务中提交。大响应先写工件，提交数据库引用；未引用成功的对象作为待清理孤儿。

    恢复时加载最后的已提交 snapshot，并核对该步骤的 model_attempts/effects。已经成功记录的模型结果不重新采样；未确认结果进入对应恢复策略。回放事件用于观察和验证 reducer，不重新执行工具。

    工具先支持 list_files、read_file、search_code、apply_patch、run_command、get_diff、finish。上下文采用任务约束 + 近期完整交互 + 按需文件片段 + 旧历史摘要；工具 schema 和输出预留都计入预算。

    build_context 在完整工具批次边界吸收补充消息；运行中的取消使用独立控制通道。设置最大模型轮数、工具次数、运行时、费用和重复无进展阈值。模型给出 finish 或最终文本都必须进入 verify。

    RepoProfile 提供可信的镜像和验证命令。初始化记录失败测试基线；修复后的可信验证证明目标条件已满足时为 verified。只有普通回归测试通过时标记 regression_only，不把它当作已证明修复。

10. 实现模型适配、流处理与共享额度。

    ```go
    type Provider interface {
        Capabilities(ctx context.Context, modelID string) (Capabilities, error)
        Stream(ctx context.Context, req ModelRequest,
            emit func(ModelEvent) error) (ModelTurn, error)
    }
    ```

    ModelRequest 包含 run_id、step_id、attempt_id、model_id、公共消息、原生状态引用、工具 schema、输出上限和 deadline。ModelTurn 包含完整工具调用、可见文本、usage、finish_reason 和原生续接状态。ModelEvent 是带类型判别的事件，不能依赖某个厂商 SDK 类型穿透整个项目。

    OpenAI 使用 Responses API 的 Go SDK；锁定 SDK 版本并对所需能力做 contract test。SDK 未覆盖的新字段只能通过小范围 adapter 扩展，不能无声丢弃。当前官方 SDK 文档提供 Go Responses 用法。[OpenAI SDKs](https://developers.openai.com/api/docs/libraries)

    Anthropic 使用 Messages 与 tool_use/tool_result 关联；厂商原生消息保留在各自 adapter 中。模型能力包括原生 compaction、tool search、原生异步工具等，按能力与组合约束启用。跨厂商切换只能在工具调用完整闭合的边界，通过公共记录和可迁移摘要重建上下文。[Anthropic Tools](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview)、[OpenAI Compaction](https://developers.openai.com/api/docs/guides/compaction)

    流式 JSON 参数必须完整组装、校验并持久化后才能执行。多个 partial tool calls 按 call_id 分别组装；限制参数大小、嵌套深度和单轮调用数量。文本增量带 attempt_id 和 provisional 标记；失败尝试通过 model.attempt_failed 标记，不与重试输出拼接成一个完整答案。

    限流、暂时服务错误采用有界指数退避和 jitter，并尊重 Retry-After。认证、非法请求和不支持能力直接失败。SDK 与应用只有一个重试责任层；已经产生 effect 的逻辑步骤不得靠重新请求模型来重做。

    每个 provider credential group 是共享额度边界。调用前在 PostgreSQL 行锁保护下同时预留并发调用槽、输入/最大输出 token 和保守费用；所有 worker 使用同一额度账本，避免各自本地限流导致总量超限。厂商真实限额以配置为准，不从客户端并发数推断。

    额度预留与调用尝试关联。完成后按实际 usage 对账；流中断时保留 estimated/unknown 状态和保守费用，不能当作免费请求。已发出的调用不会因取消而保证撤销费用。退避等待可持久化 not_before 并释放执行槽，避免 goroutine 长期空等。

    circuit breaker 按 credential group/provider 共享故障状态，配合有界重试总预算；只在兼容性与剩余预算允许时切换 provider。原生异步工具调用不替代本平台的命令执行与取消协议。[OpenAI Async Tools](https://developers.openai.com/api/docs/guides/async-tool-calling)

    并发请求槽、runner 执行槽和费用预留是不同资源。请求状态未知时不能仅凭 worker 租约过期就全部释放；按请求 deadline、provider 可查状态与保守核对策略分别处理，防止重试放大实际并发或重复返还额度。

11. 实现独立 runner 与副作用协议。

    ```proto
    service RunnerService {
      rpc PrepareWorkspace(PrepareWorkspaceRequest) returns (Workspace);
      rpc AdoptWorkspace(AdoptWorkspaceRequest) returns (Workspace);
      rpc StartOperation(StartOperationRequest) returns (Operation);
      rpc InspectOperation(InspectOperationRequest) returns (Operation);
      rpc CancelOperation(CancelOperationRequest) returns (Operation);
      rpc SealSnapshot(SealSnapshotRequest) returns (Snapshot);
      rpc ReleaseWorkspace(ReleaseWorkspaceRequest) returns (ReleaseResult);
    }
    ```

    StartOperationRequest 必须包含 tenant_id、run_id、workspace_id、operation_id、epoch、expected_revision、tool_kind、canonical_args、args_hash、policy_version、deadline 和短期执行能力令牌。所有引用类型在 proto 中显式定义，不能把协议整体压成任意 JSON。

    operation_id 在 worker 首次提交前写入 PostgreSQL effects；相同 ID/相同参数重试返回已有操作，不重新启动进程。相同 ID/不同 args_hash 返回 FAILED_PRECONDITION。StartOperation 只确认操作已持久化接收，最终结果通过 InspectOperation 查询；RPC 超时不代表命令没有启动。

    runner 在自己的 SQLite 账本中记录 prepared/running/succeeded/failed/cancelled/unknown，以及容器/job ID、epoch、工作区前后版本、日志引用、退出码和结果 hash。账本和 receipt 不挂载给任务容器。

    启动命令前写 prepared 和稳定 job 名。若启动 Docker 后写回失败，恢复时按稳定标识查找既有容器/job；确认不确定时不盲目再创建一个。优先采用每个命令有可检查容器退出状态的执行方式，避免只保存不可恢复的内存 subprocess 句柄。

    runner 按 workspace 串行处理 AdoptWorkspace 和所有写操作，并把最高 epoch 持久化。执行能力令牌由控制面在确认当前 DB lease 后签发，绑定 run/workspace/epoch/权限与期限。mTLS 识别服务身份，但不能单独证明某个 worker 当前拥有某个 run。

    能力令牌的有效期不得超过 DB lease 的剩余时间，并扣除约定的时钟偏差余量。runner 检查签名、绑定对象、期限和本地最高 epoch；不能确认授权有效性时拒绝新操作。已运行命令仍需通过接管/取消协议停止，令牌过期本身不会撤销已发生的效果。

    接受更高 epoch 时，先阻止旧 epoch 启动新操作，检查并等待或终止旧写操作，形成已停止/已完成 receipt，之后才完成新 epoch 的工作区接管。runner 不能只更新一个整数就允许新旧命令同时写。

    第一版所有工作区固定在一个 runner。该 runner 不可达时，所有依赖它的工具操作都暂停，存在未决效果的 run 进入 needs_reconciliation；控制面、事件查询及无需该 runner 的操作继续可用。恢复 runner 时先从日志和容器列表核对现场，再开放执行。SQLite 或工作区卷损坏不在基线自动恢复承诺内。

    CancelOperation 幂等地请求终止整个受控进程组/容器，完成后记录 receipt。gRPC context 取消只终止这次 RPC 等待，不作为业务取消协议。迟到的成功结果可以帮助核对效果，但不能把已经确认的终态改回 running。

    对读操作也记录实际文件 hash/revision；需要一致并发读取时使用已封存 snapshot。任意 shell 默认视为可写，单 run 内串行。跨 run 的独立工作区可并发执行。

12. 实现文件、沙箱与工件生命周期。

    仓库导入先支持用户允许的本地 snapshot。后续 HTTPS clone 使用独立 importer、来源 allowlist、大小上限与受控网络；仓库给出的 hooks/config 不能在控制进程中执行。RepoProfile 的构建镜像和验证命令由可信配置提供。

    每个 run 从固定基线导出独立工作树；控制端的原始仓库和基线记录不暴露为任务可写路径。trusted get_diff 对比真实文件与控制端基线，包含新增文件；不能相信任务自行改写的 .git 状态。

    工具输入仅接受相对路径。文件操作使用 os.Root 或等价 traversal-resistant API，限制到工作区并检查允许的文件类型。该 API 不替代进程隔离、设备访问限制和资源限制。[Go Traversal-resistant APIs](https://go.dev/blog/osroot)

    默认执行后端采用 rootless Docker，非 root 用户、只读容器根文件系统、任务专属可写工作区、独立临时目录、PID/CPU/内存上限、默认禁网、无 Docker socket。启动时检测 cgroup 与实际资源限制是否生效；达不到 profile 的要求则拒绝启动相应任务，不静默取消限制。[Docker Rootless](https://docs.docker.com/engine/security/rootless/)

    普通 rootless 容器不被描述为任意恶意代码的绝对隔离。gVisor 后端属于可选增强：分别验证系统调用兼容性、构建性能和隔离边界，不能仅更换 runtime 名称就标为已安全上线。[gVisor Security](https://gvisor.dev/docs/architecture_guide/intro/)

    apply_patch 使用 expected_file_hashes 验证前置条件，写入临时文件后完成受控发布，并记录操作前后 hash。多文件 patch 的中途失败由 journal 识别；无法证明完整应用或未应用时进入核对，不靠再次应用来猜测状态。

    工件采用 staging → ready 生命周期：先写对象并核对 hash/长度，随后在数据库事务中发布 artifact 与事件引用。失败留下的 staging 对象按保留策略清理。SQLite、工作区文件、对象存储和 PostgreSQL 之间不存在天然原子事务，所有跨存储窗口都要有核对状态。

    ArtifactStore 统一 Put、Open、Stat、Delete；首个 backend 使用持久化本地卷，由 API 在授权后代理读取。对象 key 包含 tenant/run 命名空间，但路径或内容 hash 本身不构成访问权限。

    日志设置每条、每操作和每 run 的大小上限；输出按块流式落盘并始终排空 stdout/stderr，防止管道写满死锁。超限后明确记录截断/终止策略，不能继续无限占盘。GC 不删除活跃/待核对 run 的必要工作区和 receipt。

13. 实现持久化事件与有界 SSE fanout。

    事件 schema 包含 run_id、seq、schema_version、type、step_id、attempt_id、payload、created_at。seq 在更新 run 行的短事务中分配；状态变化与对应业务事件一起提交。读取只返回已提交事件。

    事件至少包含 run.created、run.claimed、step.started、model.started、text.delta、model.attempt_failed、tool.planned、tool.finished、approval.required、quota.delayed、workspace.adopted、validation.finished 和 run.finished。

    文本按时间/大小合并后持久化，例如每 100ms 或 16KiB 一块；参数必须可配置。控制事件不能被文本合并吞掉。完整模型结果和最终输出另存工件，便于核对流中断。

    API 以 run_id/seq 从事件表分页读取，NOTIFY 仅用于降低唤醒延迟。对活跃 run 建立有界 fanout，多个订阅者共用数据库读取，不为每个客户端高频扫描同一表。

    每个订阅者设置最大待发送事件数和写超时。慢客户端超过上限就断开，由其按游标重连；不得阻塞 worker、模型流或其他客户端。事件持久化管线也必须有界：数据库不可写时停止推进新副作用，并受控停止/核对模型请求。

    GET /events 使用 Last-Event-ID。客户端按 seq 去重；心跳不占业务 seq。SSE 重连 ID 行为遵循事件流协议。[WHATWG Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)

    游标超出保留窗口返回明确的 reset_required 语义；非流式请求可用 410。GET /snapshot 在一致数据库快照下返回状态和 covered_seq，客户端随后从该 seq 继续读事件，避免 snapshot 与订阅之间丢事件。CLI 必须实现这条恢复路径。

    浏览器客户端使用支持 Authorization 的 fetch streaming，或后续独立设计的 cookie 认证；不把 bearer token 放进 URL。标准 EventSource 无法任意设置 Authorization header，不能照抄 CLI 的认证方式。

14. 完成公共 API、审批和 tenant 隔离。

    | 方法与路径 | 契约 |
    | --- | --- |
    | POST /v1/projects | 注册允许的 repo source/profile |
    | POST /v1/projects/{id}/runs | 幂等创建 run，202 返回资源引用 |
    | GET /v1/runs/{id} | 状态、version、验证状态、费用与错误 |
    | GET /v1/runs/{id}/snapshot | 一致状态及 covered_seq |
    | GET /v1/runs/{id}/events | SSE 和游标续接 |
    | POST /v1/runs/{id}/messages | 幂等追加补充指令，执行边界应用 |
    | POST /v1/runs/{id}/cancel | 幂等请求取消，返回当前状态 |
    | POST /v1/runs/{id}/resume | 带 expected_version 的恢复请求，仅对可恢复状态生效 |
    | POST /v1/approvals/{id}/decision | 审批绑定 effect、参数、revision 和 policy 版本 |
    | GET /v1/runs/{id}/artifacts | 带所有权检查的工件列表 |
    | GET /v1/artifacts/{id} | 授权后下载，支持有界分块读取 |
    | GET /healthz、GET /readyz | 进程健康与接流量能力分开 |

    API 错误格式统一为 code、message、request_id、retryable。参数错误、对象不存在、版本冲突、容量拒绝和依赖异常使用明确状态码。分页采用 keyset cursor，按 created_at+id 等稳定顺序，不对长事件流使用 OFFSET。

    初始身份方案使用管理员 CLI 签发的高熵 opaque token，数据库仅保存 hash。用户身份、tenant membership 与角色由认证/授权层解析，不能信任客户端单独提交的 tenant_id。多用户场景验证 viewer、developer、admin 的最小权限矩阵。

    PostgreSQL 为业务表增加 tenant RLS，API 连接使用非表 owner、无 BYPASSRLS 的角色，并在事务中 SET LOCAL tenant context，防止连接池遗留上下文。worker 使用独立、受控的服务角色。RLS 是第二层防护，API 仍检查对象所有权。表 owner、superuser 和 BYPASSRLS 的绕过行为必须进入部署检查与测试。[PostgreSQL Row Security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)

    审批事务检查 pending 状态、decision version、args_hash、workspace revision 与当前权限。审批后操作有变化则生成新审批。拒绝、重复审批和迟到审批都有确定结果；它们不能重新开放已结束 run。

    远程部署要求 TLS、runner mTLS 和密钥轮换配置；本地 compose 只把公共 API 暴露到 loopback。OIDC 是认证 backend 的后续替换，不改变 tenant/domain 契约。

15. 按故障矩阵实现恢复行为。

    | 故障点 | 必须发生的行为 |
    | --- | --- |
    | API 事务提交成功但响应丢失 | 同 key 重试返回同一 run |
    | worker 领取后、模型请求前退出 | 租约到期后重新领取，不丢任务 |
    | 模型流中断、usage 未返回 | 标记该尝试不完整；没有完整工具调用就不执行；费用按未知/估计核对 |
    | 完整模型响应已记录、snapshot 未推进 | 复用已记录响应，核对后推进，不再次采样已确认步骤 |
    | StartOperation RPC 超时 | 按 operation_id 查询原 runner，不直接新建 job |
    | patch 已应用、PostgreSQL effect 未完成 | 使用 runner receipt 和前后文件 hash 确认效果 |
    | 命令完成、runner receipt 未发布 | 根据稳定容器/job 标识核对退出状态；证据不足则 unknown |
    | DB 租约到期但旧命令仍在跑 | 新 worker 先完成 runner 接管/停止协议，再启动新写操作 |
    | waiting_approval 时重启 | 审批记录和工作区保留，恢复后校验参数与版本 |
    | 取消与完成同时发生 | 通过版本/事务确定合法终态；保留实际已发生的效果 |
    | PostgreSQL 短暂不可用 | 不接受无法持久化的新任务；停止启动新副作用；已有命令后续核对 |
    | runner 不可达 | 挂起所有依赖该 runner 的工具操作；控制面与事件查询继续；不在另一 runner 重放未知写入 |
    | SSE 断线/慢消费者/通知丢失 | 从持久化游标恢复，不影响任务推进 |
    | 工件写入成功但业务提交失败 | staging/orphan 状态可识别，可安全清理，不暴露为完成结果 |

    每个故障测试验证数据库最终状态、runner 实际操作次数、工作区内容、配额占用、事件序列及工件可读性。只验证“进程重新启动了”不算恢复验收。

16. 实现可观测性与运行管理。

    trace 覆盖 API 入队、队列等待、worker 步骤、模型调用、runner 操作和验证。异步执行保存/传播 trace context，并使用关联链接表达重试与接管关系。

    Prometheus 指标包含 queue_depth、dispatch_latency、active_runs、lease_expirations、reconciliation_count、provider_requests、provider_rate_limits、reserved_budget、sse_connections、slow_subscriber_disconnects、runner_operation_duration 和 artifact_bytes。

    run_id、tenant_id、文件路径、prompt hash 等高基数字段放进日志/trace，不作为默认 Prometheus label。业务费用与完成状态来自数据库账本，不能依赖可能采样或丢失的 trace。

    固定 OTel GenAI 语义约定版本，并记录映射。其官方文档已迁到专门的 GenAI 仓库，不能把旧路径当作永远稳定的字段规格。[OTel 文档迁移说明](https://opentelemetry.io/docs/specs/semconv/gen-ai/)

    明确优雅关闭顺序：API 停止接新请求；worker 停止领取、结束或移交已知操作、提交状态；runner 不启动新 job 并保留可核对现场。进程退出与业务取消是不同动作。

    migrations 使用 expand/contract：先增加兼容字段/读路径，再切换写路径，最后清理旧字段。snapshot、事件和 proto 均有 schema version；不兼容 snapshot 应暂停并给出迁移说明，不能强行按新结构解释。

    运维文档说明 PostgreSQL 备份恢复、runner journal/工作区卷的配套备份、工件保留与磁盘配额。不把数据库恢复成功等同于所有工作区恢复成功。

17. 用并发、故障和性能证据验收。

    常规 CI 使用 FakeProvider 和 FakeExecutor，真实 Docker 集成测试使用隔离 fixture。付费模型 smoke eval 通过显式命令触发并限制预算，不能混进每次基础单元测试。

    必需检查：go test ./...、关键并发包的 go test -race、reducer/参数解析/事件游标/路径的 fuzz seeds、生成代码一致性、迁移集成测试、真实 PostgreSQL 多 worker 竞争测试、真实 runner 容器集成测试。

    正确性验收包括：并发相同 key 只创建一个 run；跨 tenant 读写、SSE 和工件访问都被拒绝；重复 operation_id 不重复启动命令；旧 epoch 无法启动写操作；待核对效果不会被自动重放；取消后无受控残留进程；费用/容量不重复返还。

    性能测试分三类，报告不能混用：

    - 控制面：FakeProvider/FakeExecutor，测 API、调度、数据库、SSE 和容量控制。
    - 执行面：真实 runner、固定本地命令与文件集，测启动、日志、取消、并发隔离和资源限制。
    - 模型端到端：真实 API、小型固定代码任务，单独报告 provider 延迟与限额影响。

    建议固定参考环境为 4 vCPU、8 GiB RAM、本地 SSD；真实机器不同则记录实际规格。以下为设计验收目标，需要运行后填写实测值：

    | 工作负载 | 初始验收目标 | 范围 |
    | --- | --- | --- |
    | 50 req/s 的任务元数据读写，固定读写比例 | warm-up 后 p95 <200ms，p99 <1s | FakeProvider；不含模型与命令完成时间 |
    | 200 个 SSE 订阅、20 个模拟活跃 run、每 run 每秒 5 个 1KiB 事件块 | 无事件永久丢失，健康客户端事件延迟 p95 <1s；慢客户端被隔离 | 在事件保留窗口内允许重复交付并按 seq 去重 |
    | 两 worker 竞争 1,000 个可重复模拟任务 | 无任务永久丢失，无重复已确认 effect；实际动作次数与账本一致 | 将重复领取与重复副作用分别统计 |
    | worker 崩溃且无未决副作用 | 在一个 lease 加恢复扫描/启动开销内重新推进 | 不适用于 runner 不可达或未知效果 |
    | 连接创建/断开与任务取消反复执行 | goroutine、内存和 FD 回落至稳定范围，无持续增长趋势 | 保存 profile 和测量窗口 |

    压测文件必须固定数据规模、请求比例、事件大小、并发数、持续窗口和随机种子。记录数据库配置、连接池、binary commit、GC/pprof、样本数和原始结果；不预填“高可用”“百万 QPS”等成果。

    同时保留至少三个真实代码修复 fixture，以及一个小型独立验证任务集。成功由干净验证容器判定，目标测试和原有回归都需通过；模型输出的完成文本不算成功证据。

18. 按依赖顺序交付模块。

    | 顺序 | 交付内容 | 验收 |
    | --- | --- | --- |
    | A | domain、状态 reducer、OpenAPI/proto、sqlc、迁移、FakeProvider | 类型/协议可生成编译；状态不变量测试通过 |
    | B，依赖 A | 单进程 Agent 循环、基础工具、可信验证 | 固定任务能输出真实 patch 和验证结果 |
    | C，依赖 A/B | API、幂等提交、tenant 授权、事件存储 | 并发提交与跨 tenant 测试通过 |
    | D，依赖 C | 多 worker 调度、配额、租约、容量回收 | 多消费者领取、退避和资源计数测试通过 |
    | E，依赖 B/D | 独立 runner、操作账本、epoch 接管、取消 | 真容器内重复请求不重复执行；旧写操作核对后才接管 |
    | F，依赖 C/E | SSE fanout、游标恢复、慢消费者隔离 | 断线续接和有界资源测试通过 |
    | G，依赖 E/F | 两个原生模型 adapter、审批、补充消息、恢复矩阵 | 模型协议测试和完整故障注入通过 |
    | H，依赖 D/F/G | profiling、压测、运行文档、演示 | 交付原始测量、配置与可复现命令 |

    每个模块交付时同时提交其契约和必要验证。顺序不要求先完成所有 UI；CLI 是主要验收入口。该表表示技术依赖，不代表工期安排。

19. 为后续扩展保留明确接口。

    MCP client 放在 tool adapter 层，先接入受控的只读文档检索 server。远程授权按当前协议做 audience/resource 绑定；不得直接把平台自己的 bearer token 转发给任意 MCP server。所有远程工具仍经过本平台策略和调用账本。[MCP Authorization 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization)

    ACP adapter 先实现本地 JSON-RPC over stdio，将编辑器会话/审批/diff 映射到现有公共 API。当前 ACP 官方说明仍将完整远程支持列为进行中，因此远程产品接口继续使用项目自己的 HTTP/SSE，不宣称实现尚未核实的远程协议能力。[ACP Introduction](https://agentclientprotocol.com/get-started/introduction)

    多 runner 扩展需要增加任务放置、可信快照传输、runner 身份和容量管理；工作区迁移前必须确认旧写操作停止。S3 backend 解决工件共享，但单独增加它不能解决正在执行的工作区一致性。

    Temporal 扩展只保留为 ADR/替换实现：workflow 负责确定性控制流，模型与工具 I/O 放 Activity；继续复用 operation_id 与 runner 核对。若真正实施，必须用同一故障矩阵证明替换前后的行为契约，并删除被替换的权威调度逻辑。

    高吞吐索引、Redis 限流或消息队列只在 profiling 表明具体瓶颈后引入。若将限流转移到 Redis，需要设计主库额度账本与 Redis 状态的核对和故障语义。Kubernetes 部署属于有实际多节点需求后的独立交付。

    可增加完整 Web 客户端作为 Full-stack SDE 扩展，复用 API 与事件协议，实现 diff、审批、任务历史、运行指标和键盘操作。其验收包括前端状态续接与权限，不重复实现 Agent 循环。

20. 将交付结果与 Applied AI 路线对应起来。

    | 维度 | Applied AI 方案 | 本 SDE 方案 |
    | --- | --- | --- |
    | 主实现 | Python + LangGraph | Go + 有限状态机 + PostgreSQL |
    | 主难点 | 上下文、压缩、模型选择、评测 | 事务、并发、幂等、隔离、恢复、背压 |
    | 核心数据 | 任务成功率、每成功任务成本、消融实验 | 正确性不变量、恢复行为、时延/吞吐/资源曲线 |
    | 模型角色 | 主要实验对象 | 有限额与不确定性的外部依赖 |
    | 运行边界 | 单用户、单机效果闭环 | 受控多用户、多 worker、独立 runner |
    | 首要证据 | 固定任务集、逐任务结果、失败分析 | 故障注入、协议测试、race/fuzz、压测与 profile |

    两份文档可以复用任务定义、RepoProfile、工具 schema 的逻辑设计、FakeProvider fixtures 和验证方法；不能直接混用数据库 migration 或同时让 LangGraph 与 Go runtime 推进同一 run。

    如果后续整合两条路线，推荐选择一个权威编排层：例如保留 Python Applied AI runtime，复用本方案的 Go runner 作为独立执行后端。此时 LangGraph 管理 run，Go runner 管理 operation；接口边界与所有权必须重新写清楚。

    最终仓库应包含源码、锁定依赖、迁移、OpenAPI/proto、compose、故障测试、压测脚本、原始数据、ADR 和运行手册。演示包含一次修复、一次 worker 崩溃恢复、一次慢客户端隔离、一次跨 tenant 拒绝和一份真实压测报告。

    简历指标从实测填写。例如：Built a Go-based coding-agent execution platform with idempotent APIs, fenced worker leases, isolated runners, and resumable event streams; validated [measured workload] and [fault scenarios] with [measured outcomes].

    面试解释应能够从一次具体请求出发，说明它在哪个事务被接受、谁有权推进、命令结果存在哪里、连接或进程失败后如何判定事实，以及压测中的瓶颈如何被证据定位。
