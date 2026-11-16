export type TaskState = 'queued' | 'running' | 'stopping' | 'succeeded' | 'failed' | 'cancelled'
export type StopKind = 'preempt' | 'pause' | 'cancel' | 'timeout' | 'unregister' | 'close'
export type ResourceDemand = Readonly<Record<string, number>>

export class TaskInterrupted extends Error {
  constructor(readonly kind: StopKind, message = `Task interrupted: ${kind}`) {
    super(message)
    this.name = 'TaskInterrupted'
  }
}

export interface Execution<C> {
  readonly signal: AbortSignal
  readonly attempt: number
  readonly checkpoint: C | undefined
  /** 仅保存已经完成的阶段；恢复时由执行器解释，不克隆资源句柄。 */
  saveCheckpoint(value: C): void
  /** 提交外部结果前调用；业务持久层仍须检查资源版本与任务世代。 */
  assertCurrent(): void
}

export interface TaskDefinition<I, O, C = never> {
  readonly name: string
  key(input: I): string
  resources(input: I): ResourceDemand
  /** restartable 必须响应 signal 并等待实际退出；atomic 只在边界结束。 */
  readonly mode: 'restartable' | 'atomic'
  execute(input: I, execution: Execution<C>): Promise<O>
  readonly timeoutMs?: number
}

export interface DemandOptions {
  priority: number
  signal?: AbortSignal
  order?: 'fifo' | 'lifo'
  /** 仅用于可重新发现的后台工作。所有活跃需求均允许驱逐时才能淘汰。 */
  evictable?: boolean
}

export interface TaskLease<O> {
  readonly id: number
  readonly result: Promise<O>
  setPriority(priority: number): void
  release(reason?: string): void
}

export interface TaskQueue<I, O> {
  submit(input: I, options: DemandOptions): TaskLease<O>
  /** 注销立刻停止接单；Promise 等待执行端实际退出。 */
  close(): Promise<void>
}

export interface SchedulerOptions {
  resources: ResourceDemand
  concurrency?: number
  capacity?: number
  agingMs?: number
  quantumMs?: number
  historyLimit?: number
}

export interface TaskSnapshot {
  id: number
  type: string
  key: string
  state: TaskState
  mode: 'restartable' | 'atomic'
  priority: number
  demandCount: number
  resources: ResourceDemand
  attempt: number
  createdAt: number
  waitedMs: number
  stop?: StopKind
}

export interface TaskEvent {
  sequence: number
  time: number
  action: 'queued' | 'demand' | 'started' | 'interrupt' | 'resumed' | 'succeeded' | 'failed' | 'cancelled'
  task: TaskSnapshot
  error?: string
}
