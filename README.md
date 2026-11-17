# Cordis Task Scheduler

TypeScript 任务调度器，提供多环分派、共享需求、资源额度、协作式抢占、lane 串行隔离、阶段屏障和可选 SQLite 持久队列。核心可以单独使用，也可以通过 Cordis 插件管理生命周期。

## 安装与入口

```sh
pnpm add https://github.com/leolee9086/cordis-task-scheduler/releases/download/v0.1.0-alpha.4/cordis-task-scheduler-0.1.0-alpha.4.tgz
```

| 入口 | 用途 |
| --- | --- |
| `@leolee9086/cordis-task-scheduler` | `TaskScheduler`、任务契约与错误类型 |
| `@leolee9086/cordis-task-scheduler/plugin` | Cordis `taskScheduler` 服务、`tasks:changed` 与 `tasks:gap` 事件 |
| `@leolee9086/cordis-task-scheduler/worker` | Node Worker 执行器 |
| `@leolee9086/cordis-task-scheduler/process` | Node IPC 子进程执行器 |
| `@leolee9086/cordis-task-scheduler/sqlite` | `SqliteTaskStore` 持久收件箱 |

Cordis 插件需要 `@deepseek-ai/cordis` 4.0.5 或兼容版本。SQLite 入口接收调用方创建的 `node:sqlite` `DatabaseSync`；核心入口不加载 SQLite。真实文件验证环境为 Node 22.19.0。

## 从一个真实文件任务开始

```ts
import { createReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { TaskScheduler } from '@leolee9086/cordis-task-scheduler'

const scheduler = new TaskScheduler({
  resources: { disk: 1 }, concurrency: 2, capacity: 256,
  defaultRing: 'background', quantumMs: 100,
  rings: [
    { name: 'interactive', rank: 0, preempts: ['background'] },
    { name: 'background', rank: 1, maxBypass: 8 },
  ],
})
const digests = scheduler.register<{ path: string; revision: string }, string>({
  name: 'file.sha256', mode: 'restartable',
  key: input => JSON.stringify([input.path, input.revision]),
  resources: () => ({ disk: 1 }),
  execute: async (input, execution) => {
    const hash = createHash('sha256')
    for await (const chunk of createReadStream(input.path, { signal: execution.signal })) {
      hash.update(chunk)
    }
    execution.assertCurrent()
    return hash.digest('hex')
  },
})

const path = process.argv[2]
if (!path) throw new Error('请传入一个已有文件路径')
const info = await stat(path)
const source = { path, revision: JSON.stringify([info.size, info.mtimeMs]) }
// 示例使用 stat 版本；生产存储摘要前仍需再次核对资源身份和版本。
const lease = digests.submit(source, { ring: 'interactive', priority: 50 })
try { console.log(await lease.result) }
finally { lease.release(); await scheduler.close() }
```

## 环、需求与准入

环间先比较 `rank`，数值越小越先分派；同 rank 的就绪环轮转。每个环分别配置 `order: 'priority' | 'fifo' | 'lifo'`、容量 `capacity`、并发 `concurrency`、资源上限 `resources` 与老化周期 `agingMs`。未提供 `rings` 时使用一个名为 `default` 的环。

环内 `priority` 范围为 -100 到 100。默认按优先级加等待老化排序，相同权重按请求的 FIFO/LIFO 次序处理。`preempts` 明确允许中断哪些其他环；`preemptWithin` 控制同环优先级抢占。普通抢占至少等待本次执行达到 `quantumMs`。

`maxBypass` 是等待环可以让过的其他分派次数预算。到期后优先获得一次服务机会，也可抢占其他环的可恢复任务。公平执行有保护窗口，至少一个 quantum；没有检查点的任务反复重启时，窗口随累计重启成本增长。这个策略帮助有限任务取得连续服务，也可能增加前台等待。原子任务或不响应中断的执行器仍会阻塞资源，因此这里没有墙钟延迟上界。

同一注册队列的同 key 工作共享执行，每次 `submit` 得到独立租约：

- `result` 保留成功或拒绝结果；取得租约后立即接入 await/then/catch，主动释放也会拒绝，不能弃置结果。
- `setDemand({ ring, priority })` 更新这一份需求。
- `release(reason?)` 撤销这一份需求；最后一份撤销才取消工作。

有效需求取静态环级顺序最强的一份，再比较环内优先级。同 key 的 lane、segment 和资源向量必须一致；影响工作内容的版本和参数应进入 key。结果不会在核心中缓存。

`submitBatch(requests, expectedVersion?)` 先验证并预留容量，再发布整批。`capacityBehavior` 可设为 `evict`、`reject` 或 `throw`：evict 只驱逐有效需求较弱、全部租约均允许 `evictable` 的等待工作；reject 返回整批拒绝租约；throw 同步抛出容量错误。其他验证错误均同步抛出。容量按首次准入环计费，需求提权不会改变积压归属。`queue.version` 与 `cancelPending(id, expectedVersion)` 提供等待任务的乐观并发检查。

## 执行、中断与依赖

资源向量整体分配，执行 Promise 真正退出后归还额度。`execution.signal` 通知抢占、取消、暂停或关闭，执行器必须等待自己持有的资源停止。

| 模式 | 契约 |
| --- | --- |
| `restartable` | 响应中断并清理；抢占或暂停后从头或检查点重试 |
| `atomic` | 普通抢占和暂停不打断执行；取消仍通知 signal，额度保持到退出 |
| `orchestrator` | 只编排工作，资源必须为空，不占执行并发，但占任务容量 |

`lane(input)` 定义跨任务类型互斥的串行域。`segment(input)` 定义本注册队列的非负整数阶段；前一阶段全部实际退出，后面阶段才开始。开始过的阶段不能再插入更早阶段；失败或取消也算该项已退出，若要求“前段成功才执行”，由编排器处理业务失败。

编排器使用 `execution.submitChild(queue, input)` 等待同一调度器的子任务。子需求继承当前及后续需求变化，父执行结束自动释放；依赖环、祖先 lane 和祖先阶段造成的等待会被拒绝。持有执行资源的普通任务不能使用此入口。

跨服务传递需求时使用 `execution.demand` 和 `execution.onDemandChange(listener)`。监听器同步收到当前值，后续变化在整批准入完成后、下一次分派前合并交付。`saveCheckpoint(value)` 仅保存当前任务的内存进度。提交业务结果前调用 `assertCurrent()`，持久层还应检查业务版本与领取凭据。

## SQLite 持久收件箱

创建 `SqliteTaskStore({ database, namespace, codec })`；codec 必须提供明确的 `version`、同步 `encode(input)` 与 `decode(json)`，不推测旧字段。连接、journal、同步策略和关闭由调用方管理；队列拥有自己的 `scheduler_namespaces`、`scheduler_tasks` 表。

| 操作 | 事务语义 |
| --- | --- |
| `submitBatch` | 全批入库或回滚；同 key 必须保持相同 payload 和 demand |
| `updatePending` / `promote` / `cancelPending` | 按队列版本检查，修改等待工作并更新 revision |
| `pending(limit)` | 按入队 sequence 枚举有限候选，不提前领取 |
| `claim(candidates, owner)` | 检查每项 revision，整批领取并生成 token |
| `settle(claim, outcome, commit?)` | 校验 owner/token/revision，同事务写业务结果与终态 |
| `release(claim)` | 校验凭据后回到 pending，旧凭据失效 |
| `recover(owner, policy)` | 调用方确认这个旧 owner 已退出后，明确选择 retry 或 fail |
| `prune(retain)` | 清理已结算任务，保留活跃任务及业务结果表 |

推荐在调度器执行器实际取得额度后 `claim`，执行真实工作，检查资源版本和当前执行，再以 `settle` 同事务提交结果。`version` 随成功变更增长；失败会回滚。已完成任务需要新的业务 key 才能代表新的工作。

codec 必须是纯同步转换，不得重入队列、修改输入或访问数据库。settle 回调只能同步写当前连接的业务表，不得自行 BEGIN/COMMIT/ROLLBACK 或改写调度器表。队列操作不能嵌套在调用方已打开的事务里；需要原子业务写入时使用 settle 回调。回调不能执行不可回滚的外部副作用。

恢复不会按心跳超时推断进程死亡，也不会自动重放未知副作用。数据库事务只覆盖同一连接中的写入；外部网络或文件操作的幂等性由业务实现。运行快照和事件历史不用于持久恢复。

## 生命周期、执行器与观测

`queue.close()` 同步停止接单并让出注册名，返回的 Promise 等待旧执行退出。新代可以立即注册，同 key/lane 执行仍等待旧代释放。资源和队列创建后应立即登记 Cordis 清理，确保初始化中途失败也能撤销贡献。

`onIdle({ signal?, timeoutMs? })` 等待实际空闲。`drain()` 停止外部接单，让已有工作及其子任务完成；原来暂停的调度器仍保持暂停。`pause()` 中断可恢复工作，`resume()` 继续分派，`close()` 取消全部工作并等待退出。

WorkerExecutor 取消时等待 `terminate()` 完成。ChildProcessExecutor 发送 SIGKILL 并等待 close；它只拥有直接子进程，工厂不能留下无人管理的孙进程。不可中断的原生调用持续占用额度，需要强制停止时应放进可终止的独立执行端。

`subscribe({ event, gap })` 返回退订函数。事件与诊断共用有界历史，溢出通过 gap 明确报告缺失 sequence 范围；收到 gap 后按需用 `snapshot()` 重建视图。快照包含任务、资源用量、环状态、六类索引计数和历史。事件中的异常保留 stack、cause 与 AggregateError 子错误。

## 从 alpha.3 更新

移除 `execution.priority`、`onPriorityChange`、`lease.setPriority`，消费端统一使用完整 demand。订阅入口改为 `subscribe({ event, gap })`；队列 close 现在立即让出注册名。所有消费端应同步更新，不提供旧接口兼容层。

## 许可

AGPL-3.0-or-later。
