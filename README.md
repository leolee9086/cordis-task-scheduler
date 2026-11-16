# Cordis Task Scheduler

TypeScript 任务调度核心与 Cordis 插件。统一交互、可见资源、预取及后台任务的身份、需求、资源分派和中断生命周期。包名 `@leolee9086/cordis-task-scheduler`，计划发布到 https://github.com/leolee9086/cordis-task-scheduler 。

## 调度契约

任务类型由插件注册，声明输入类型、输出类型、身份键、资源需求与执行函数。同一注册实例的相同身份键共享一项工作；每个调用者持有独立需求租约，取消只撤销自己的需求。最后一个租约释放才取消工作，优先级取活跃需求的最高值。身份键必须包含会改变输出的资源版本与参数，调度器不推测路径身份。

优先级数值越大越优先，同级默认 FIFO；后台可显式选 LIFO。等待时间持续增加调度权重，已获得公平性提升的任务在本次执行中保留该权重，避免刚启动就被再次抢占。队列容量有界；只有可驱逐的等待任务才能被更高优先级的新任务替换，运行任务不因容量不足被暗中删除。

每次执行领取一组资源额度，整体原子分配；释放只发生在执行 Promise 真正结束之后。中断发出 AbortSignal 并进入 stopping 状态；它不等于完成。执行器必须等待原生资源停止、Worker terminate 或进程退出，才能结束 Promise。不能中断的原子阶段必须显式声明为 atomic，调度器等待其完成，观测信息保留实际模式。

可恢复任务调用 checkpoint 保存应用自有状态。抢占后重新排队，下一次执行取得最新检查点。检查点不自动回滚磁盘或网络副作用；任务通过 assertCurrent 在提交前核验执行代次，并使用业务自身的事务/世代检查提交。不可重入副作用应使用 atomic 模式。

取消、超时、抢占、卸载是不同原因。超时会中断执行，但在执行端退出前继续持有额度，避免把仍占资源的任务从计数中抹去。插件注销取消其全部任务并等待实际清理。事件与有界历史可观测等待、启动、中断请求、恢复、完成和失败。

## 参考与取舍

- SACAssetsManager `source/server/processors/queue/taskQueue.js`：吸收前台与后台分级、后台 LIFO、堆式优先选择；其全局 pause 不会中断正在 await 的任务。前台请求直接改全局布尔值会互相覆盖，改由独立租约表达需求。
- SACAssetsManager `src/toolBox/base/useAsync/forTaskQueue/forSerialTaskQueue.js`：吸收显式暂停/恢复与容量边界；idle callback 只能决定何时开始，不能代表执行端暂停。
- textureBackerDemo `packages/my-async-queue/src/index.ts`：吸收任务状态机、公开结果与内部执行结算分离、并发/容量控制。其 running cancel 只拒绝公开 Promise，载荷仍继续；本库保留执行占用直到确认退出。
- 同库 `transaction.ts`：队列内存快照不能撤销已发生的外部副作用，也不能回滚执行中的 Promise；本库使用检查点与业务事务，不提供误导性的队列事务回滚。
- s-forge `packages/agentqueue/ring.go` 与 `kernel/api/magi_priority_queue.go`：吸收投递语义与优先级分离、同级 FIFO、关闭后禁止入队。真实消费者 `kernel/api/magi.go` 的 submitMagiTaskWithReplyStream 先调用 InterruptHeartbeat 再入前台队列；`kernel/api/magi_runtime.go` 会 cancel 活跃心跳并等待 done，最多等待 1500ms。统一分发器仍等 CoordinateHeartbeat 返回才取下一项，不能把这个等待超时当作执行完成。本库将中断原因、执行退出回执与重新排队放入统一状态机。

参考项目暂不做消费者改造；新库实现并发布后，在原队列入口添加迁移注释，要求后续切换到本库。参考项目原有协议不被直接冒充为兼容 API。

## 开发与交付

使用 pnpm、严格 TypeScript；不引入 any，不运行 mock 或生造数据测试。仅做类型检查、构建及真实文件/应用负载验证。消费者只允许 registry 或 GitHub 固定版本依赖，禁止 file:、link: 及 workspace: 跨仓库依赖。

## 使用边界

从主入口导入 TaskScheduler，或将 `@leolee9086/cordis-task-scheduler/plugin` 挂到 Cordis。配置必须给出资源额度，例如 cpu、disk、network；名称由应用决定。任务通过 register 注册后，由返回队列的 submit 获取租约。租约 result 返回结果，setPriority 更新自己的需求优先级，release 撤销自己的需求。

CPU 密集任务可以使用 `@leolee9086/cordis-task-scheduler/worker` 的 WorkerExecutor。构造器接收 Worker 工厂与响应校验函数。Worker 必须每个输入只发一个最终响应；进度消息须由另一个协议通道处理。执行器不维护内部任务队列，调用方先通过调度器领取额度。

任务 execute 不得在持有额度时再等待领取同一组额度的子任务，否则会形成资源死锁。分阶段工作应由外部编排在上一阶段完成、释放额度后再提交下一阶段，或声明不同资源。检查点用于同一任务被抢占后的恢复。长时间不响应 AbortSignal 的原子调用无法由 JavaScript 强行中断；需要强制中断时必须放进 Worker 或独立进程。

首个预发布版本已通过严格类型检查、构建，以及真实 JPEG/PNG 的 Worker 抢占重启、共享任务、单租约撤销、文件读取检查点恢复和 Cordis 卸载验证。尚未接入 FileBrowser；应用可见任务插队、颜色索引、公平性持续负载和所有任务入口迁移仍待验收。详细事实见仓库 `docs/2026-11-17-validation.md`。
