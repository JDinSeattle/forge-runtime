# Forge Runtime：可以追问和复现的项目经历

项目定位：面向代码修改任务的 Go Agent 执行平台。重点是任务中断后如何恢复、
如何判断工具是否真正执行、以及如何用独立验证证明补丁有效。
这是个人工程项目；下述数据来自本机受控测试，不代表雇主经历或线上业务指标。

## 可以展示的完整路径

CLI 提交固定源码版本 → API 校验租户与幂等键 → PostgreSQL 持久化任务 →
worker 领取有期限的租约 → 保存模型结果和工具意图 → 独立 runner 执行 →
干净容器运行可信测试 → 发布补丁、验证报告和操作回执 → CLI 下载并校验哈希。

已经通过这条真实链路运行三个 Python 修复任务：clamp 边界、TTL 到期时刻、
闭区间相接。每个任务先证明原实现失败，再验证修复后的 target 和 regression，
最后把导出的 Git patch 应用到另一份源码并核对结果。
模型使用确定性脚本，因此这些结果证明平台与验证链路，不衡量模型推理能力。
对应原始产物见 [repair report](../benchmarks/results/repairs-20260908T080253Z/report.json)。

## 一个值得深入讲的故障

**触发点：worker 已保存完整模型结果，但还没把下一状态提交给 PostgreSQL。**

若恢复时重新问一次模型，新回复可能产生不同操作；若直接重复工具调用，
也可能产生第二次写入。Forge 把模型结果、工具意图和执行回执分别持久化，
接管者根据已有事实继续执行，而不是把超时理解为“没有执行”。

实测让 worker 在该位置直接 `os.Exit(86)`，另两个独立 worker 进程等待
30 秒租约自然到期后竞争接管。任务在提交后 32.876 秒完成，epoch 从 1 升到 2；
三个模型步骤各只有一次 attempt，所有 execution allocation 已释放，
剩余 quota reservation 为零。见 [进程退出日志](../benchmarks/results/worker-crash-20260908.log)、
[数据库事实](../benchmarks/results/worker-crash-ledger-20260908.json) 和
[接管后的补丁报告](../benchmarks/results/repairs-20260908T080852Z/report.json)。

这个实验覆盖“保存模型结果后的进程崩溃”。它没有证明任意时刻掉电、磁盘丢失
或跨主机切换都能恢复。解释这个边界，比声称实现了普遍的 exactly-once 更准确。

## 能经得起实现追问的设计

| 追问 | 实现与取舍 |
| --- | --- |
| 租约为什么不够？ | 数据库 epoch 能拒绝旧 worker 的状态写入，但无法自动停止已运行的容器。runner 另有持久化 fence、停止回执与 adoption 屏障。 |
| RPC 超时后怎么办？ | 固定 runner、稳定 operation ID；先 Inspect 原操作。已有相同意图返回原回执，不创建新的逻辑写入。未知结果暂停依赖操作。 |
| 两个存储怎样保持一致？ | 不假装 PostgreSQL 与 SQLite/文件存在原子事务。保存意图、发布不可变对象、提交引用，跨存储间隙由检查与恢复状态处理。 |
| 为什么不用全套消息队列？ | 当前测量规模使用 PostgreSQL SKIP LOCKED 与短事务即可；单 runner 行争用已在原始 benchmark 中保留，可据此决定是否拆调度热点。 |
| 怎么控制预算？ | 调用前冻结价格策略并预留额度；调用后按已知 usage 结算。未知 usage 保留保守负债，释放并发不等于退还费用。 |
| 怎么限制不可信代码？ | rootless Docker、非 root 任务 UID、无网络、只读 rootfs、固定 ext4 卷、cgroup 上限；可信 grader 的断言位于只读工作区之外。 |
| 为什么清理需要协议？ | 执行槽释放不等于可以删除工作区。单独 cleanup lease 先停止、封存并发布快照，再释放物理卷；中断后按阶段重试。 |

真实容器测试中，相同写操作重复请求返回同一回执，文件计数为 1；
256 MiB 内存上限触发 `OOMKilled=true`，PID 限制返回 EAGAIN，写满固定卷返回
ENOSPC，取消父子进程后 cgroup 任务数为 0。查看
[原始限制与回执日志](../benchmarks/results/real-docker-limits-20260908.log)。

## 简历表述示例

- 使用 Go、PostgreSQL、gRPC 和 SQLite 构建代码 Agent 执行平台，通过持久化意图、
  epoch fencing 与操作回执协调 worker 崩溃后的恢复；真实进程退出实验验证已保存的模型步骤不会被重新调用。
- 实现租户 RLS、幂等提交、额度预留结算、可恢复 SSE 与独立容器验证；
  在本机 50 请求/秒的 1,000 次元数据读写样本中，测得 p95 4.1 ms、零错误。
- 为任务配置固定容量 ext4 工作区及 rootless 容器限制，验证 OOM、PID 限额、
  ENOSPC 和进程树取消，并通过三个真实容器修复用例重放导出的补丁。

投递时只使用自己能够运行、解释并修改的部分；SSE/terminal simulation 的新结果
应以 [证据账本](evidence.md) 为准。不要把 fake model 成功率改写为真实模型成功率，
也不要把单机样本改写成线上吞吐、成本节省、用户规模或工作经历。
