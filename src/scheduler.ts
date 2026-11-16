import { IndexedHeap } from './heap.js'
import { TaskInterrupted, type DemandOptions, type ResourceDemand, type SchedulerOptions, type StopKind,
  type TaskDefinition, type TaskEvent, type TaskLease, type TaskQueue, type TaskSnapshot, type TaskState } from './types.js'

interface Demand {
  priority: number
  evictable: boolean
  reject(error: Error): void
  cleanup(): void
}
interface Task {
  id: number
  type: string
  key: string
  state: TaskState
  mode: TaskDefinition<unknown, unknown>['mode']
  resources: ResourceDemand
  demands: Map<symbol, Demand>
  priority: number
  priorityListeners: Set<(priority: number) => void>
  order: 'fifo' | 'lifo'
  score: number
  createdAt: number
  queuedAt: number
  waitedMs: number
  dispatchWeight: number
  startedAt: number
  attempt: number
  controller?: AbortController
  stop?: TaskInterrupted
  timeoutMs?: number
  execution?: Promise<void>
  execute(signal: AbortSignal, assertCurrent: () => void): Promise<() => void>
  remove(): void
}
const terminal = (state: TaskState): boolean => ['succeeded', 'failed', 'cancelled'].includes(state)
const positive = (value: number, name: string): number => {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`)
  return value
}
const priorityValue = (value: number): number => {
  if (!Number.isFinite(value) || value < -100 || value > 100) throw new Error('Priority must be between -100 and 100')
  return value
}
const errorOf = (error: unknown): Error => error instanceof Error ? error : new Error(String(error))

/** 调度核心不依赖 Cordis，也不认识文件、颜色或网络协议。 */
export class TaskScheduler {
  private readonly tasks = new Map<number, Task>()
  private readonly running = new Set<Task>()
  private readonly registrations = new Map<string, () => Promise<void>>()
  private readonly pending = new IndexedHeap<Task>((a, b) => b.score - a.score
    || (a.order === 'lifo' ? -a.id : a.id) - (b.order === 'lifo' ? -b.id : b.id))
  private readonly used: Record<string, number> = Object.create(null) as Record<string, number>
  private readonly limits: ResourceDemand
  private readonly concurrency: number
  private readonly capacity: number
  private readonly agingMs: number
  private readonly quantumMs: number
  private readonly historyLimit: number
  private readonly history: TaskEvent[] = []
  private readonly listeners = new Set<(event: TaskEvent) => void>()
  private nextId = 0
  private sequence = 0
  private scheduled = false
  private paused = false
  private closed = false
  private closing?: Promise<void>
  private timer?: ReturnType<typeof setTimeout>

  constructor(options: SchedulerOptions) {
    this.concurrency = positive(options.concurrency ?? 4, 'concurrency')
    this.capacity = positive(options.capacity ?? 512, 'capacity')
    this.agingMs = positive(options.agingMs ?? 1000, 'agingMs')
    this.quantumMs = positive(options.quantumMs ?? 100, 'quantumMs')
    this.historyLimit = positive(options.historyLimit ?? 256, 'historyLimit')
    const resources: Record<string, number> = Object.create(null) as Record<string, number>
    for (const [name, amount] of Object.entries(options.resources)) {
      if (!name) throw new Error('Resource name is empty')
      resources[name] = positive(amount, `resource ${name}`)
      this.used[name] = 0
    }
    this.limits = Object.freeze(resources)
  }

  register<I, O, C = never>(definition: TaskDefinition<I, O, C>): TaskQueue<I, O> {
    if (this.closed) throw new Error('Scheduler is closed')
    if (!definition.name || this.registrations.has(definition.name)) throw new Error(`Task type already registered or empty: ${definition.name}`)
    if (definition.timeoutMs !== undefined) positive(definition.timeoutMs, 'timeoutMs')
    const entries = new Map<string, { task: Task; subscribe(options: DemandOptions): TaskLease<O> }>()
    let closed = false
    let closing: Promise<void> | undefined
    const close = (): Promise<void> => {
      if (closing) return closing
      closed = true
      // 同 key 的新世代可能已经覆盖 entries，旧世代仍须等执行退出。
      const active = [...this.tasks.values()].filter(task => task.type === definition.name)
      for (const task of active) this.cancel(task, new TaskInterrupted('unregister'))
      closing = Promise.all(active.map(task => task.execution)).then(() => {
        if (this.registrations.get(definition.name) === close) this.registrations.delete(definition.name)
      })
      return closing
    }
    this.registrations.set(definition.name, close)
    return {
      close,
      submit: (input, options) => {
        if (closed || this.closed) throw new Error('Task type is closed')
        priorityValue(options.priority)
        options.signal?.throwIfAborted()
        const key = definition.key(input)
        if (!key) throw new Error('Task identity must be nonempty')
        const existing = entries.get(key)
        const predecessor = existing?.task.stop && !['preempt', 'pause'].includes(existing.task.stop.kind)
          ? existing.task : undefined
        if (existing && !predecessor) return existing.subscribe(options)
        const demand = Object.create(null) as Record<string, number>
        for (const [resource, amount] of Object.entries(definition.resources(input))) {
          positive(amount, `demand ${resource}`)
          if (this.limits[resource] === undefined || amount > this.limits[resource]!) throw new Error(`Unavailable resource: ${resource}`)
          demand[resource] = amount
        }
        if (definition.mode === 'orchestrator' && Object.keys(demand).length) {
          throw new Error('Orchestrators cannot reserve execution resources')
        }
        this.makeRoom(options.priority)
        const successes = new Map<symbol, (value: O) => void>()
        let checkpoint: C | undefined
        const now = performance.now()
        const task: Task = {
          id: ++this.nextId, type: definition.name, key, state: 'queued', mode: definition.mode,
          resources: Object.freeze(demand), demands: new Map(), priority: options.priority,
          priorityListeners: new Set(),
          order: options.order ?? 'fifo', score: options.priority - now / this.agingMs,
          createdAt: Date.now(), queuedAt: now, waitedMs: 0, dispatchWeight: options.priority,
          startedAt: 0, attempt: 0, timeoutMs: definition.timeoutMs,
          execute: async (signal, assertCurrent) => {
            try {
              const value = await definition.execute(input, {
                signal, attempt: task.attempt, checkpoint,
                get priority() { return task.priority },
                onPriorityChange: listener => {
                  assertCurrent()
                  listener(task.priority)
                  task.priorityListeners.add(listener)
                  return () => { task.priorityListeners.delete(listener) }
                },
                saveCheckpoint: value => { assertCurrent(); checkpoint = value }, assertCurrent,
              })
              return () => { for (const resolve of successes.values()) resolve(value) }
            } finally { task.priorityListeners.clear() }
          },
          remove: () => { if (entries.get(key)?.task === task) entries.delete(key); successes.clear() },
        }
        const subscribe = (request: DemandOptions): TaskLease<O> => {
          priorityValue(request.priority)
          request.signal?.throwIfAborted()
          const token = Symbol()
          let release = (_reason?: string): void => {}
          const result = new Promise<O>((resolve, reject) => {
            const onAbort = (): void => release('Demand aborted')
            const consumer: Demand = {
              priority: request.priority, evictable: request.evictable === true,
              reject, cleanup: () => { request.signal?.removeEventListener('abort', onAbort); successes.delete(token) },
            }
            release = (reason = 'Demand released') => {
              if (!task.demands.delete(token)) return
              consumer.cleanup()
              reject(new TaskInterrupted('cancel', reason))
              if (!task.demands.size) this.cancel(task, new TaskInterrupted('cancel', reason))
              else { this.updatePriority(task); this.emit('demand', task); this.wake() }
            }
            successes.set(token, resolve)
            task.demands.set(token, consumer)
            request.signal?.addEventListener('abort', onAbort, { once: true })
          })
          // 租约可以只用于维持需求；挂内部观察者避免无人 await 时出现未处理拒绝。
          void result.catch(() => {})
          this.updatePriority(task)
          this.emit('demand', task)
          this.wake()
          return { id: task.id, result, release, setPriority: priority => {
            priorityValue(priority)
            const consumer = task.demands.get(token)
            if (!consumer) return
            consumer.priority = priority
            this.updatePriority(task); this.emit('demand', task); this.wake()
          } }
        }
        entries.set(key, { task, subscribe })
        this.tasks.set(task.id, task)
        const lease = subscribe(options)
        this.emit('queued', task)
        return lease
      },
    }
  }

  private updatePriority(task: Task): void {
    const previous = task.priority
    task.priority = Math.max(...[...task.demands.values()].map(demand => demand.priority))
    if (!Number.isFinite(task.priority)) return
    if (task.state === 'queued') {
      task.score = task.priority + Math.floor((task.waitedMs + performance.now() - task.queuedAt) / this.agingMs)
      this.pending.upsert(task)
    } else task.dispatchWeight += task.priority - previous
    if (task.priority !== previous) {
      for (const listener of task.priorityListeners) {
        try { listener(task.priority) } catch (error) {
          this.cancel(task, new TaskInterrupted('cancel', `Priority propagation failed: ${errorOf(error).message}`))
          break
        }
      }
    }
  }

  private makeRoom(priority: number): void {
    if (this.tasks.size < this.capacity) return
    let worst: Task | undefined
    for (const task of this.pending.values()) {
      if (task.priority >= priority || [...task.demands.values()].some(demand => !demand.evictable)) continue
      if (!worst || task.score < worst.score) worst = task
    }
    if (!worst) throw new Error('Scheduler capacity exceeded')
    this.cancel(worst, new TaskInterrupted('cancel', 'Evicted by higher priority demand'))
  }

  private executionCount(released: readonly Task[] = []): number {
    return [...this.running].filter(task => task.mode !== 'orchestrator' && !released.includes(task)).length
  }

  private fits(task: Task, released: readonly Task[] = []): boolean {
    // 取消后的重订阅会创建新世代；无论其间撤销过几次，都要等同键旧执行真正退出。
    if ([...this.running].some(previous => previous.type === task.type && previous.key === task.key)) return false
    if (task.mode === 'orchestrator') return true
    if (this.executionCount(released) >= this.concurrency) return false
    return Object.entries(task.resources).every(([resource, amount]) => {
      const freed = released.reduce((sum, victim) => sum + (victim.resources[resource] ?? 0), 0)
      return (this.used[resource] ?? 0) - freed + amount <= this.limits[resource]!
    })
  }

  private wake(): void {
    if (this.scheduled || this.closed) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }

  private pump(): void {
    if (this.closed || this.paused) return
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    const agedAt = performance.now()
    for (const task of this.pending.values()) {
      task.score = task.priority + Math.floor((task.waitedMs + agedAt - task.queuedAt) / this.agingMs)
    }
    this.pending.rebuild()
    for (;;) {
      const first = this.pending.peek()
      if (!first) return
      if (this.fits(first)) { this.start(first); continue }
      const now = performance.now(), weight = first.score
      const victims: Task[] = []
      // 已请求停止的执行仍占额度，但可以算进即将释放的容量，避免反复中断更多任务。
      const candidates = [...this.running].filter(task => task.mode !== 'orchestrator' && (task.state === 'stopping'
        || (task.mode === 'restartable' && now - task.startedAt >= this.quantumMs && task.dispatchWeight < weight)))
        .sort((a, b) => Number(b.state === 'stopping') - Number(a.state === 'stopping') || a.dispatchWeight - b.dispatchWeight)
      for (const task of candidates) {
        const conflicts = this.executionCount(victims) >= this.concurrency
          || Object.keys(first.resources).some(resource => (task.resources[resource] ?? 0) > 0)
        if (conflicts) victims.push(task)
        if (this.fits(first, victims)) break
      }
      if (victims.length && this.fits(first, victims)) {
        for (const task of victims) this.interrupt(task, new TaskInterrupted('preempt'))
      }
      // 无关资源仍可前进；与队首共享额度的低优先任务不能抢走它正在等待的资源。
      let independent: Task | undefined
      for (const task of this.pending.values()) {
        if (task === first || !this.fits(task) || Object.keys(task.resources).some(key => key in first.resources)) continue
        if (!independent || task.score > independent.score) independent = task
      }
      if (independent) { this.start(independent); continue }
      if (this.running.size) this.timer = setTimeout(() => { this.timer = undefined; this.pump() }, this.quantumMs)
      return
    }
  }

  private start(task: Task): void {
    this.pending.remove(task.id)
    const now = performance.now()
    task.waitedMs += now - task.queuedAt
    task.dispatchWeight = task.priority + Math.floor(task.waitedMs / this.agingMs)
    task.startedAt = now
    task.state = 'running'; task.stop = undefined; task.attempt++
    const controller = new AbortController()
    task.controller = controller
    this.running.add(task)
    for (const [resource, amount] of Object.entries(task.resources)) this.used[resource] = (this.used[resource] ?? 0) + amount
    const assertCurrent = (): void => {
      controller.signal.throwIfAborted()
      if (task.controller !== controller || task.state !== 'running') throw new TaskInterrupted('cancel', 'Execution is no longer current')
    }
    const timer = task.timeoutMs === undefined ? undefined : setTimeout(() => {
      this.cancel(task, new TaskInterrupted('timeout'))
    }, task.timeoutMs)
    this.emit('started', task)
    // 延后 execute，使同步完成/抛错也通过同一个结算路径。
    task.execution = Promise.resolve().then(async () => {
      let deliver: (() => void) | undefined
      let failure: Error | undefined
      try {
        assertCurrent()
        deliver = await task.execute(controller.signal, assertCurrent)
        assertCurrent()
      } catch (error) { failure = errorOf(error) }
      // 执行 Promise 已结束，才释放额度并交付公开结果；close 也等待此清理边界。
      if (timer) clearTimeout(timer)
      this.running.delete(task)
      for (const [resource, amount] of Object.entries(task.resources)) this.used[resource] = this.used[resource]! - amount
      task.controller = undefined
      if (task.stop && ['preempt', 'pause'].includes(task.stop.kind) && task.demands.size && !this.closed) {
        task.state = 'queued'; task.queuedAt = performance.now(); task.stop = undefined
        this.updatePriority(task)
        this.emit('resumed', task)
      } else if (failure || task.stop) {
        this.finish(task, task.stop && task.stop.kind !== 'timeout' ? 'cancelled' : 'failed', task.stop ?? failure!)
      } else {
        deliver!()
        this.finish(task, 'succeeded')
      }
      this.wake()
    })
  }

  private interrupt(task: Task, reason: TaskInterrupted): void {
    if (task.state !== 'running') return
    task.state = 'stopping'; task.stop = reason
    // 状态先改变再发 signal，执行器的同步 abort 回调可以观察到 stopping。
    task.controller!.abort(reason)
    this.emit('interrupt', task)
  }

  private cancel(task: Task, reason: TaskInterrupted): void {
    if (terminal(task.state)) return
    for (const demand of task.demands.values()) { demand.cleanup(); demand.reject(reason) }
    task.demands.clear()
    if (task.state === 'queued') this.finish(task, reason.kind === 'timeout' ? 'failed' : 'cancelled', reason)
    else if (task.state === 'running') this.interrupt(task, reason)
    else { task.stop = reason; this.emit('interrupt', task) }
    this.wake()
  }

  private finish(task: Task, state: 'succeeded' | 'failed' | 'cancelled', error?: Error): void {
    task.state = state
    this.pending.remove(task.id)
    for (const demand of task.demands.values()) {
      demand.cleanup()
      if (error) demand.reject(error)
    }
    task.demands.clear()
    this.emit(state, task, error)
    this.tasks.delete(task.id)
    task.remove()
  }

  pause(): void {
    this.paused = true
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    for (const task of this.running) if (task.mode !== 'atomic') this.interrupt(task, new TaskInterrupted('pause'))
  }
  resume(): void {
    if (this.closed) throw new Error('Scheduler is closed')
    this.paused = false; this.wake()
  }
  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    for (const task of [...this.tasks.values()]) this.cancel(task, new TaskInterrupted('close'))
    this.closing = Promise.all([...this.running].map(task => task.execution)).then(() => {
      this.registrations.clear(); this.listeners.clear()
    })
    return this.closing
  }
  subscribe(listener: (event: TaskEvent) => void): () => void {
    if (this.closed) throw new Error('Scheduler is closed')
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }
  snapshot(): { paused: boolean; closed: boolean; resources: ResourceDemand; used: ResourceDemand; tasks: TaskSnapshot[]; history: TaskEvent[] } {
    return { paused: this.paused, closed: this.closed, resources: { ...this.limits }, used: { ...this.used },
      tasks: [...this.tasks.values()].map(task => this.describe(task)), history: structuredClone(this.history) }
  }
  private describe(task: Task): TaskSnapshot {
    return { id: task.id, type: task.type, key: task.key, state: task.state, mode: task.mode,
      priority: task.priority, demandCount: task.demands.size, resources: { ...task.resources },
      attempt: task.attempt, createdAt: task.createdAt,
      waitedMs: task.waitedMs + (task.state === 'queued' ? performance.now() - task.queuedAt : 0),
      ...(task.stop ? { stop: task.stop.kind } : {}) }
  }
  private emit(action: TaskEvent['action'], task: Task, error?: Error): void {
    const event: TaskEvent = { sequence: ++this.sequence, time: Date.now(), action, task: this.describe(task),
      ...(error ? { error: error.message } : {}) }
    this.history.push(event)
    if (this.history.length > this.historyLimit) this.history.shift()
    // 观察回调不能在状态迁移中途重入 close/submit，事件保留原来的顺序与快照。
    for (const listener of this.listeners) queueMicrotask(() => {
      if (!this.listeners.has(listener)) return
      try { listener(structuredClone(event)) } catch (error) { console.error('[task-scheduler] observer failed', error) }
    })
  }
}
