import type { RingPolicy } from './dispatch.js'
export type { RingPolicy } from './dispatch.js'
export type TaskState = 'queued' | 'running' | 'stopping' | 'succeeded' | 'failed' | 'cancelled'
export type StopKind = 'preempt' | 'pause' | 'cancel' | 'timeout' | 'unregister' | 'close'
export type ResourceDemand = Readonly<Record<string, number>>
export interface SchedulingDemand { ring: string; priority: number }
export type CapacityBehavior = 'evict' | 'reject' | 'throw'

export class TaskInterrupted extends Error {
  constructor(readonly kind: StopKind, message = `Task interrupted: ${kind}`, options?: ErrorOptions) {
    super(message, options)
    this.name = 'TaskInterrupted'
  }
}
export class TaskCapacityError extends Error {
  constructor(readonly ring?: string) {
    super(ring ? `Ring capacity exceeded: ${ring}` : 'Scheduler capacity exceeded')
    this.name = 'TaskCapacityError'
  }
}
export class QueueVersionConflict extends Error {
  constructor(readonly expected: number, readonly actual: number) {
    super(`Queue version conflict: expected ${expected}, actual ${actual}`)
    this.name = 'QueueVersionConflict'
  }
}

export interface Execution<C> {
  readonly signal: AbortSignal
  readonly attempt: number
  readonly demand: Readonly<SchedulingDemand>
  /** 同步交付当前值；后续变化在完整准入之后、下一次分派之前合并交付，执行结束自动解绑。 */
  onDemandChange(listener: (demand: Readonly<SchedulingDemand>) => void): () => void
  /** 仅 orchestrator 可向同一调度器提交子需求，继承需求并在父执行结束释放；拒绝依赖环及祖先 lane/阶段阻塞。 */
  submitChild<I, O>(queue: TaskQueue<I, O>, input: I): TaskLease<O>
  readonly checkpoint: C | undefined
  /** 仅保存已完成阶段；内存检查点不会自动变成持久化记录。 */
  saveCheckpoint(value: C): void
  /** 提交外部结果前调用；持久层仍须检查资源版本与任务世代。 */
  assertCurrent(): void
}

export interface TaskDefinition<I, O, C = never> {
  readonly name: string
  key(input: I): string
  resources(input: I): ResourceDemand
  /** 同名 lane 跨任务类型互斥；没有 lane 的任务仅按自身 key 去重。 */
  lane?(input: I): string | undefined
  /** 非负整数阶段；上一阶段的所有任务实际退出后才能开启下一阶段。 */
  segment?(input: I): number | undefined
  /** restartable 必须响应 signal 并等待实际退出；atomic 只在边界结束。
   * orchestrator 只编排独立子任务，resources 必须为空，不占执行并发。 */
  readonly mode: 'restartable' | 'atomic' | 'orchestrator'
  execute(input: I, execution: Execution<C>): Promise<O>
  readonly timeoutMs?: number
}

export interface DemandOptions {
  priority: number
  /** 省略时进入 SchedulerOptions.defaultRing。 */
  ring?: string
  signal?: AbortSignal
  order?: 'fifo' | 'lifo'
  /** 所有活跃需求均允许驱逐，等待任务才可淘汰。 */
  evictable?: boolean
}
export interface TaskLease<O> {
  readonly id: number
  readonly result: Promise<O>
  setDemand(demand: SchedulingDemand): void
  release(reason?: string): void
}
export interface IdleOptions { signal?: AbortSignal; timeoutMs?: number }
export interface SubmitRequest<I> { input: I; options: DemandOptions }
export interface TaskQueue<I, O> {
  readonly version: number
  submit(input: I, options: DemandOptions): TaskLease<O>
  /** 先完整验证及预留容量，再发布整批；版本冲突或容量不足不产生部分入队。
   * capacityBehavior=reject 时整批返回 rejected leases，其余策略容量不足同步抛错。
   * 同 key 的 lane/segment/resources 必须一致；payload 版本由调用方纳入 key。 */
  submitBatch(requests: readonly SubmitRequest<I>[], expectedVersion?: number): TaskLease<O>[]
  /** 乐观并发取消，只取消仍在等待的任务；已经开始执行时拒绝变更。 */
  cancelPending(id: number, expectedVersion: number): void
  onIdle(options?: IdleOptions): Promise<void>
  /** 注销立刻停止接单并让出注册名；Promise 等待本代执行真正退出。 */
  close(): Promise<void>
}

export interface SchedulerOptions {
  resources: ResourceDemand
  concurrency?: number
  capacity?: number
  capacityBehavior?: CapacityBehavior
  agingMs?: number
  quantumMs?: number
  historyLimit?: number
  rings?: readonly RingPolicy[]
  defaultRing?: string
}
export interface TaskSnapshot {
  id: number
  type: string
  key: string
  state: TaskState
  mode: TaskDefinition<unknown, unknown>['mode']
  ring: string
  priority: number
  lane?: string
  segment?: number
  demandCount: number
  resources: ResourceDemand
  attempt: number
  createdAt: number
  waitedMs: number
  stop?: StopKind
}
/** 观测事件复用有界历史；订阅方收到 gap 后应通过 snapshot 重建自己的视图。 */
export interface TaskEventGap { fromSequence: number; toSequence: number }
export interface TaskObserver {
  event(event: TaskEvent): void
  gap(gap: TaskEventGap): void
}
export interface TaskEvent {
  sequence: number
  time: number
  action: 'queued' | 'demand' | 'started' | 'interrupt' | 'resumed' | 'succeeded' | 'failed' | 'cancelled'
  task: TaskSnapshot
  error?: string
}
