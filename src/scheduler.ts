import { IndexedHeap } from './heap.js'
import { DispatchIndex } from './dispatch.js'
import { QueueVersionConflict, TaskCapacityError, TaskInterrupted, type CapacityBehavior,
  type DemandOptions, type IdleOptions, type ResourceDemand, type SchedulerOptions, type SchedulingDemand,
  type SubmitRequest, type TaskDefinition, type TaskEvent, type TaskLease, type TaskQueue,
  type TaskSnapshot, type TaskState, type TaskObserver } from './types.js'

interface Demand extends SchedulingDemand {
  id: number
  evictable: boolean
  reject(error: Error): void
  cleanup(): void
}
interface Task extends SchedulingDemand {
  id: number
  type: string
  key: string
  state: TaskState
  mode: TaskDefinition<unknown, unknown>['mode']
  resources: ResourceDemand
  resourceProfile: string
  registration: number
  dependencies: Map<Task, number>
  started(): void
  admissionRing: string
  lane?: string
  segment?: number
  segmentBlocker(): string | undefined
  demands: IndexedHeap<Demand>
  protectedDemands: number
  listeners: Set<(demand: Readonly<SchedulingDemand>) => void>
  order: 'fifo' | 'lifo'
  score: number
  createdAt: number
  queuedAt: number
  waitedMs: number
  dispatchWeight: number
  startedAt: number
  protectedUntil: number
  restartCostMs: number
  attempt: number
  controller?: AbortController
  stop?: TaskInterrupted
  timeoutMs?: number
  execution?: Promise<void>
  execute(signal: AbortSignal, assertCurrent: () => void): Promise<() => void>
  changed(): void
  remove(): void
}
const terminal = (state: TaskState): boolean => state === 'succeeded' || state === 'failed' || state === 'cancelled'
const positive = (value: number, name: string): number => {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`)
  return value
}
const priorityValue = (value: number): number => {
  if (!Number.isFinite(value) || value < -100 || value > 100) throw new Error('Priority must be between -100 and 100')
  return value
}
const errorOf = (error: unknown): Error => error instanceof Error ? error : new Error(String(error), { cause: error })
const errorDetails = (error: unknown, seen = new Set<unknown>()): string => {
  if (seen.has(error)) return '[circular error cause]'
  seen.add(error)
  if (!(error instanceof Error)) return String(error)
  const parts = [error.stack ?? `${error.name}: ${error.message}`]
  if (error.cause !== undefined) parts.push(`Caused by: ${errorDetails(error.cause, seen)}`)
  if (error instanceof AggregateError) {
    for (const [index, nested] of error.errors.entries()) parts.push(`Error ${index + 1}: ${errorDetails(nested, seen)}`)
  }
  return parts.join('\n')
}

/** 调度核心不依赖 Cordis，也不认识文件、颜色或网络协议。 */
export class TaskScheduler {
  private readonly tasks = new Map<number, Task>()
  private readonly running = new Set<Task>()
  private readonly ownedQueues = new WeakSet<object>()
  /** drain 只封闭外部接单；已运行编排器仍可提交完成现有工作必需的子任务。 */
  private readonly childSubmissions = new WeakSet<DemandOptions>()
  private readonly registrations = new Map<string, () => Promise<void>>()
  private readonly dispatch: DispatchIndex<Task>
  private readonly evictable: IndexedHeap<Task>
  private readonly ringEvictable = new Map<string, IndexedHeap<Task>>()
  private readonly limits: ResourceDemand
  private readonly capacity: number
  private readonly capacityBehavior: CapacityBehavior
  private readonly defaultRing: string
  private readonly quantumMs: number
  private readonly historyLimit: number
  private readonly history: TaskEvent[] = []
  private historyCursor = 0
  private readonly listeners = new Map<TaskObserver, { after: number }>()
  private notifying = false
  private readonly idleWaiters = new Set<() => void>()
  private readonly changedDemands = new Set<Task>()
  private nextId = 0
  private nextDemand = 0
  private nextRegistration = 0
  private sequence = 0
  private scheduled = false
  private paused = false
  private closed = false
  private closing?: Promise<void>
  private timer?: ReturnType<typeof setTimeout>

  constructor(options: SchedulerOptions) {
    const concurrency = positive(options.concurrency ?? 4, 'concurrency')
    this.capacity = positive(options.capacity ?? 512, 'capacity')
    this.capacityBehavior = options.capacityBehavior ?? 'evict'
    this.quantumMs = positive(options.quantumMs ?? 100, 'quantumMs')
    this.historyLimit = positive(options.historyLimit ?? 256, 'historyLimit')
    const resources: Record<string, number> = Object.create(null) as Record<string, number>
    for (const [name, amount] of Object.entries(options.resources)) {
      if (!name) throw new Error('Resource name is empty')
      resources[name] = positive(amount, `resource ${name}`)
    }
    this.limits = Object.freeze(resources)
    const policies = options.rings ?? [{ name: 'default', rank: 0 }]
    this.defaultRing = options.defaultRing ?? policies[0]?.name ?? ''
    this.dispatch = new DispatchIndex(policies, this.limits, concurrency,
      positive(options.agingMs ?? 1000, 'agingMs'), this.quantumMs)
    this.dispatch.policy(this.defaultRing)
    this.evictable = new IndexedHeap((a, b) => this.dispatch.compareEviction(a, b))
    for (const policy of policies) this.ringEvictable.set(policy.name,
      new IndexedHeap((a, b) => this.dispatch.compareEviction(a, b)))
  }

  private normalize(options: DemandOptions): SchedulingDemand {
    const demand = { ring: options.ring ?? this.defaultRing, priority: priorityValue(options.priority) }
    this.dispatch.policy(demand.ring)
    options.signal?.throwIfAborted()
    return demand
  }
  private validateResources(resources: ResourceDemand, ring: string): void {
    const policy = this.dispatch.policy(ring)
    for (const [name, amount] of Object.entries(resources)) {
      positive(amount, `demand ${name}`)
      if (this.limits[name] === undefined || amount > this.limits[name]!
        || amount > (policy.resources?.[name] ?? this.limits[name]!)) throw new Error(`Unavailable resource in ring ${ring}: ${name}`)
    }
  }

  register<I, O, C = never>(definition: TaskDefinition<I, O, C>): TaskQueue<I, O> {
    if (this.closed) throw new Error('Scheduler is closed')
    if (!definition.name || this.registrations.has(definition.name)) throw new Error(`Task type already registered or empty: ${definition.name}`)
    if (definition.timeoutMs !== undefined) positive(definition.timeoutMs, 'timeoutMs')
    const registration = ++this.nextRegistration
    const entries = new Map<string, { task: Task; subscribe(options: DemandOptions): TaskLease<O> }>()
    const active = new Set<Task>()
    const stages = new IndexedHeap<{ id: number; count: number }>((a, b) => a.id - b.id)
    const stageCounts = new Map<number, { id: number; count: number }>()
    const waiters = new Set<() => void>()
    let closed = false, version = 0, highestStartedStage = -1
    let closing: Promise<void> | undefined
    const changed = (): void => { version++ }
    const checkVersion = (expected: number | undefined): void => {
      if (expected !== undefined && expected !== version) throw new QueueVersionConflict(expected, version)
    }
    const assertOpen = (internal = false): void => {
      if (closed || this.closed || (!this.accepting && !internal)) throw new Error('Task type is not accepting work')
    }
    const close = (): Promise<void> => {
      if (closing) return closing
      closed = true
      // 注册名即刻让出；旧代 active 集合保持独立，同键运行索引仍保护新代。
      if (this.registrations.get(definition.name) === close) this.registrations.delete(definition.name)
      for (const task of [...active]) this.cancel(task, new TaskInterrupted('unregister'))
      closing = Promise.all([...active].map(task => task.execution)).then(() => {})
      return closing
    }

    const create = (input: I, key: string, resources: ResourceDemand, options: DemandOptions,
      lane: string | undefined, segment: number | undefined): { task: Task; subscribe(options: DemandOptions): TaskLease<O> } => {
      const successes = new Map<number, (value: O) => void>()
      let checkpoint: C | undefined
      const now = performance.now(), demand = this.normalize(options)
      const task: Task = {
        id: ++this.nextId, type: definition.name, key, resources, lane, segment, registration,
        resourceProfile: JSON.stringify([definition.mode === 'orchestrator', Object.entries(resources).sort(([a], [b]) => a.localeCompare(b))]),
        dependencies: new Map(),
        started: () => { if (segment !== undefined) highestStartedStage = Math.max(highestStartedStage, segment) },
        state: 'queued', mode: definition.mode, ...demand, admissionRing: demand.ring, protectedDemands: 0,
        demands: new IndexedHeap((a, b) => this.dispatch.compareDemand(a, b) || a.id - b.id),
        listeners: new Set(), order: options.order ?? 'fifo', score: demand.priority - now / this.dispatch.aging(demand.ring),
        createdAt: Date.now(), queuedAt: now, waitedMs: 0, dispatchWeight: demand.priority,
        startedAt: 0, protectedUntil: 0, restartCostMs: 0, attempt: 0, timeoutMs: definition.timeoutMs, changed,
        segmentBlocker: () => segment !== undefined && stages.peek()?.id !== segment ? `segment:${registration}` : undefined,
        execute: async (signal, assertCurrent) => {
          const children = new Set<() => void>()
          const onDemandChange = (listener: (value: Readonly<SchedulingDemand>) => void): (() => void) => {
            assertCurrent(); listener({ ring: task.ring, priority: task.priority }); task.listeners.add(listener)
            return () => { task.listeners.delete(listener) }
          }
          try {
            const value = await definition.execute(input, {
              signal, attempt: task.attempt, checkpoint,
              get demand() { return { ring: task.ring, priority: task.priority } },
              onDemandChange,
              submitChild: (queue, childInput) => {
                assertCurrent()
                if (task.mode !== 'orchestrator') throw new Error('Only orchestrators may submit child tasks')
                if (!this.ownedQueues.has(queue)) throw new Error('Child queue must belong to the same scheduler')
                const options: DemandOptions = { ring: task.ring, priority: task.priority, signal }
                this.childSubmissions.add(options)
                const lease = (() => {
                  try { return queue.submit(childInput, options) }
                  finally { this.childSubmissions.delete(options) }
                })()
                const child = this.tasks.get(lease.id)
                if (!child) return lease // 容量 reject 返回已拒绝租约，不建立不存在的依赖。
                try { this.assertDependency(task, child) }
                catch (error) {
                  // 此租约尚未交给调用方；内部撤销产生的预期取消在这里消费，
                  // 真正的依赖错误仍向父执行抛出，其他失败必须保留诊断。
                  void lease.result.catch(failure => {
                    if (!(failure instanceof TaskInterrupted) || failure.kind !== 'cancel') {
                      console.error('[task-scheduler] rejected child cleanup failed', failure)
                    }
                  })
                  lease.release('Invalid child dependency'); throw error
                }
                task.dependencies.set(child, (task.dependencies.get(child) ?? 0) + 1)
                const unlink = onDemandChange(value => lease.setDemand(value))
                let disposed = false
                const cleanup = (): void => {
                  if (disposed) return
                  disposed = true
                  unlink(); lease.release('Parent execution ended'); children.delete(cleanup)
                  const count = task.dependencies.get(child)!
                  if (count === 1) task.dependencies.delete(child)
                  else task.dependencies.set(child, count - 1)
                }
                children.add(cleanup)
                void lease.result.then(cleanup, cleanup)
                return lease
              },
              saveCheckpoint: value => { assertCurrent(); checkpoint = value; task.restartCostMs = 0 }, assertCurrent,
            })
            return () => { for (const resolve of successes.values()) resolve(value) }
          } finally {
            for (const cleanup of children) cleanup()
            task.listeners.clear()
          }
        },
        remove: () => {
          if (entries.get(key)?.task === task) entries.delete(key)
          active.delete(task); successes.clear()
          if (segment !== undefined) {
            const stage = stageCounts.get(segment)!
            if (!--stage.count) { stageCounts.delete(segment); stages.remove(segment); this.dispatch.wake(`segment:${registration}`) }
          }
          changed()
          if (!active.size) for (const notify of [...waiters]) notify()
        },
      }
      const subscribe = (request: DemandOptions): TaskLease<O> => {
        const effective = this.normalize(request), id = ++this.nextDemand
        let release = (_reason?: string): void => {}
        let consumer!: Demand
        const result = new Promise<O>((resolve, reject) => {
          const onAbort = (): void => release('Demand aborted')
          consumer = { id, ...effective, evictable: request.evictable === true, reject,
            cleanup: () => { request.signal?.removeEventListener('abort', onAbort); successes.delete(id) } }
          release = (reason = 'Demand released') => {
            if (!task.demands.remove(id)) return
            if (!consumer.evictable) task.protectedDemands--
            consumer.cleanup(); reject(new TaskInterrupted('cancel', reason))
            if (!task.demands.size) this.cancel(task, new TaskInterrupted('cancel', reason))
            else { this.updateDemand(task); this.emit('demand', task); this.wake() }
          }
          successes.set(id, resolve); task.demands.upsert(consumer)
          if (!consumer.evictable) task.protectedDemands++
          request.signal?.addEventListener('abort', onAbort, { once: true })
        })
        // result 的失败由租约消费者观察；不挂空 catch 吞掉遗漏的失败通知。
        this.updateDemand(task); this.emit('demand', task); this.wake()
        const setDemand = (value: SchedulingDemand): void => {
          if (!successes.has(id)) return
          const next = this.normalize(value)
          this.validateResources(task.resources, next.ring)
          Object.assign(consumer, next); task.demands.upsert(consumer)
          this.updateDemand(task); this.emit('demand', task); this.wake()
        }
        return { id: task.id, result, release, setDemand }
      }
      return { task, subscribe }
    }

    const submitBatch = (requests: readonly SubmitRequest<I>[], expectedVersion?: number): TaskLease<O>[] => {
      const internal = requests.length > 0 && requests.every(request => this.childSubmissions.has(request.options))
      assertOpen(internal); checkVersion(expectedVersion)
      const preparedVersion = version
      // 第一阶段只验证、创建未发布的候选。定义回调失败不会留下部分队列。
      const plans = new Map<string, { entry: ReturnType<typeof create>; fresh: boolean; demand: SchedulingDemand }>()
      const prepared: { entry: ReturnType<typeof create>; options: DemandOptions }[] = []
      for (const request of requests) {
        const demand = this.normalize(request.options), key = definition.key(request.input)
        if (!key) throw new Error('Task identity must be nonempty')
        const resources = Object.freeze({ ...definition.resources(request.input) })
        this.validateResources(resources, demand.ring)
        if (definition.mode === 'orchestrator' && Object.keys(resources).length) throw new Error('Orchestrators cannot reserve execution resources')
        const lane = definition.lane?.(request.input), segment = definition.segment?.(request.input)
        if (lane === '') throw new Error('Lane must be nonempty')
        if (segment !== undefined && (!Number.isSafeInteger(segment) || segment < 0 || segment < highestStartedStage)) throw new Error('Segment must be a nonnegative integer no earlier than the active stage')
        let plan = plans.get(key)
        if (!plan) {
          const existing = entries.get(key)
          const reusable = existing && (!existing.task.stop || existing.task.stop.kind === 'preempt' || existing.task.stop.kind === 'pause')
          if (reusable) plan = { entry: existing, fresh: false, demand: { ring: existing.task.ring, priority: existing.task.priority } }
          else plan = { entry: create(request.input, key, resources, request.options, lane, segment), fresh: true, demand }
          plans.set(key, plan)
        }
        // 相同 key 表示同一工作；调用方必须将会改变工作的版本纳入 key。
        // 调度约束可以直接核验，禁止悄悄用首个请求覆盖后续 lane/阶段/资源。
        const shared = plan.entry.task
        if (shared.lane !== lane || shared.segment !== segment
          || Object.keys(shared.resources).length !== Object.keys(resources).length
          || Object.entries(resources).some(([name, amount]) => shared.resources[name] !== amount)) {
          throw new Error(`Conflicting scheduling constraints for shared task: ${key}`)
        }
        if (this.dispatch.compareDemand(demand, plan.demand) < 0) plan.demand = demand
        prepared.push({ entry: plan.entry, options: request.options })
      }
      // 用户回调可能重入；发布之前再次检查队列版本和关闭状态。
      assertOpen(internal); checkVersion(preparedVersion)
      for (const item of prepared) item.options.signal?.throwIfAborted()
      this.reserve([...plans.values()].filter(plan => plan.fresh).map(plan => ({ ...plan.demand })),
        new Set([...plans.values()].filter(plan => !plan.fresh).map(plan => plan.entry.task.id)))
      for (const plan of plans.values()) if (plan.fresh) {
        const { task } = plan.entry
        task.ring = plan.demand.ring; task.admissionRing = plan.demand.ring; task.priority = plan.demand.priority
        entries.set(task.key, plan.entry); active.add(task); this.tasks.set(task.id, task); this.dispatch.add(task)
        if (task.segment !== undefined) {
          let stage = stageCounts.get(task.segment)
          if (!stage) { stage = { id: task.segment, count: 0 }; stageCounts.set(task.segment, stage) }
          stage.count++; stages.upsert(stage)
        }
      }
      const leases = prepared.map(item => item.entry.subscribe(item.options))
      for (const plan of plans.values()) if (plan.fresh) this.emit('queued', plan.entry.task)
      return leases
    }
    const queue: TaskQueue<I, O> = {
      get version() { return version }, close,
      submitBatch: (requests, expectedVersion) => {
        try { return submitBatch(requests, expectedVersion) }
        catch (error) {
          if (!(error instanceof TaskCapacityError) || this.capacityBehavior !== 'reject') throw error
          return requests.map(() => {
            const result = Promise.reject<O>(error)
            return { id: 0, result, release: () => {}, setDemand: () => {} }
          })
        }
      },
      submit: (input, options) => queue.submitBatch([{ input, options }])[0]!,
      cancelPending: (id, expectedVersion) => {
        assertOpen(); checkVersion(expectedVersion)
        const task = this.tasks.get(id)
        if (!task || !active.has(task) || task.state !== 'queued') throw new Error('Task is not pending in this queue')
        this.cancel(task, new TaskInterrupted('cancel', 'Cancelled by versioned queue mutation'))
      },
      onIdle: options => this.waitIdle(() => active.size === 0, waiters, options),
    }
    this.registrations.set(definition.name, close)
    this.ownedQueues.add(queue)
    return queue
  }

  /** 检查显式等待图，以及运行祖先持有的 lane/阶段屏障。只在新增子需求时遍历。 */
  private assertDependency(parent: Task, child: Task): void {
    const reaches = (start: Task, target: Task): boolean => {
      const pending = [start], visited = new Set<Task>()
      while (pending.length) {
        const current = pending.pop()!
        if (current === target) return true
        if (visited.has(current)) continue
        visited.add(current)
        pending.push(...current.dependencies.keys())
      }
      return false
    }
    if (reaches(child, parent)) throw new Error('Child dependency would create a cycle')
    for (const ancestor of this.running) {
      if (!reaches(ancestor, parent)) continue
      if (ancestor.lane !== undefined && child.lane === ancestor.lane) throw new Error('Child dependency is blocked by an ancestor lane')
      if (ancestor.registration === child.registration && ancestor.segment !== undefined
        && child.segment !== undefined && child.segment > ancestor.segment) throw new Error('Child dependency is blocked by an ancestor segment')
    }
  }

  /** 容量变更先规划，只有全部满足才驱逐；溢出路径不会影响正常入队热路径。 */
  private reserve(changes: readonly SchedulingDemand[], excluded: ReadonlySet<number>): void {
    let total = this.tasks.size
    const counts = new Map<string, number>(), arrivals = new Map<string, SchedulingDemand>()
    const getCount = (ring: string): number => counts.get(ring) ?? this.dispatch.count(ring)
    for (const change of changes) {
      total++; counts.set(change.ring, getCount(change.ring) + 1)
      const previous = arrivals.get(change.ring)
      if (!previous || this.dispatch.compareDemand(change, previous) < 0) arrivals.set(change.ring, change)
    }
    const victims = new Set<Task>()
    const choose = (heap: IndexedHeap<Task>, incoming: SchedulingDemand): Task | undefined => {
      // 暂时移出不合格候选后原位恢复；只在溢出时访问，避免常态扫描。
      const skipped: Task[] = []
      let selected: Task | undefined
      for (;;) {
        const task = heap.peek()
        if (!task) break
        if (!excluded.has(task.id) && !victims.has(task) && this.dispatch.compareDemand(incoming, task) < 0) { selected = task; break }
        skipped.push(heap.remove(task.id)!)
      }
      for (const task of skipped) heap.upsert(task)
      return selected
    }
    for (const [ring, count] of counts) {
      let excess = count - (this.dispatch.policy(ring).capacity ?? this.capacity)
      while (excess > 0) {
        const victim = this.capacityBehavior === 'evict' ? choose(this.ringEvictable.get(ring)!, arrivals.get(ring)!) : undefined
        if (!victim) throw new TaskCapacityError(ring)
        victims.add(victim); total--; excess--
      }
    }
    let strongest: SchedulingDemand | undefined
    for (const arrival of arrivals.values()) if (!strongest || this.dispatch.compareDemand(arrival, strongest) < 0) strongest = arrival
    while (total > this.capacity) {
      const victim = this.capacityBehavior === 'evict' && strongest ? choose(this.evictable, strongest) : undefined
      if (!victim) throw new TaskCapacityError()
      victims.add(victim); total--
    }
    for (const task of victims) this.cancel(task, new TaskInterrupted('cancel', 'Evicted by higher scheduling demand'))
  }

  private updateDemand(task: Task): void {
    const demand = task.demands.peek()
    if (!demand) return
    const previous = { ring: task.ring, priority: task.priority }
    this.unindexEviction(task)
    if (task.ring !== demand.ring) this.dispatch.move(task, demand.ring)
    task.priority = demand.priority
    if (task.state === 'queued') {
      // 同环所有等待任务共享 now/agingMs 项，比较时消去，无需定时重建堆。
      task.score = task.priority + (task.waitedMs - task.queuedAt) / this.dispatch.aging(task.ring)
      this.dispatch.enqueue(task)
      if (!task.protectedDemands) { this.evictable.upsert(task); this.ringEvictable.get(task.admissionRing)!.upsert(task) }
    } else task.dispatchWeight = task.priority + task.waitedMs / this.dispatch.aging(task.ring)
    if (task.priority !== previous.priority || task.ring !== previous.ring) this.changedDemands.add(task)
  }
  private propagateDemands(): void {
    // 在完整准入事务之后、下一次分派之前交付，回调无法打断半批发布。
    while (this.changedDemands.size) {
      const task = this.changedDemands.values().next().value!
      this.changedDemands.delete(task)
      if (task.state !== 'running') continue
      for (const listener of task.listeners) {
        try { listener({ ring: task.ring, priority: task.priority }) }
        catch (error) { this.cancel(task, new TaskInterrupted('cancel', `Demand propagation failed: ${errorOf(error).message}`, { cause: error })); break }
      }
    }
  }
  private unindexEviction(task: Task): void {
    this.evictable.remove(task.id); this.ringEvictable.get(task.admissionRing)!.remove(task.id)
  }
  private wake(): void {
    if (this.scheduled || this.closed) return
    this.scheduled = true
    queueMicrotask(() => { this.scheduled = false; this.pump() })
  }
  private pump(): void {
    this.propagateDemands()
    if (this.closed || this.paused) return
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    // 每次 pump 都重新纳入资源组头部。新请求不能借另一次 pump 绕过既有队首；
    // 这里只扫描受阻资源组，不扫描组内任务或 key/lane/segment 等待集合。
    this.dispatch.retryBlocked()
    const reserved = new Map<string, Task>()
    for (;;) {
      const next = this.dispatch.next()
      if (!next) break
      const { task, fair } = next
      const stage = task.segmentBlocker()
      if (stage) { this.dispatch.block(task, stage); continue }
      const blocker = this.dispatch.blocker(task)
      if (blocker?.startsWith('key:') || blocker?.startsWith('lane:')) {
        this.dispatch.block(task, blocker); continue
      }
      // 一项多资源工作需要先积累全部额度；同组更高优先级和到期公平环在前面选中。
      // 注册到先行任务的等待原因，先行任务取消/启动/退出后必然重新评估。
      const scopes = task.mode === 'orchestrator' ? [] : [
        'concurrency', `ring:${task.ring}`, ...Object.keys(task.resources).map(name => `resource:${name}`),
      ]
      const owner = scopes.map(scope => reserved.get(scope)).find(value => value !== undefined)
      if (owner) { this.dispatch.block(task, `reservation:${owner.id}`); continue }
      if (!blocker) { this.start(task, fair); continue }
      for (const victim of this.dispatch.victims(task, fair, performance.now())) this.interrupt(victim, new TaskInterrupted('preempt'))
      if (blocker === 'concurrency' || blocker.startsWith('ring:')) reserved.set(blocker, task)
      for (const name of Object.keys(task.resources)) reserved.set(`resource:${name}`, task)
      this.dispatch.block(task, blocker)
    }
    if (this.running.size && this.dispatch.hasBlockedGroups) this.timer = setTimeout(() => {
      this.timer = undefined; this.dispatch.retryBlocked(); this.pump()
    }, this.quantumMs)
  }

  private start(task: Task, fair: boolean): void {
    const now = performance.now()
    this.dispatch.acquire(task, fair, now); this.unindexEviction(task)
    task.waitedMs += now - task.queuedAt
    task.dispatchWeight = task.priority + task.waitedMs / this.dispatch.aging(task.ring)
    task.startedAt = now
    task.started()
    task.state = 'running'; task.stop = undefined; task.attempt++
    const controller = new AbortController()
    task.controller = controller; this.running.add(task)
    const assertCurrent = (): void => {
      controller.signal.throwIfAborted()
      if (task.controller !== controller || task.state !== 'running') throw new TaskInterrupted('cancel', 'Execution is no longer current')
    }
    const timer = task.timeoutMs === undefined ? undefined : setTimeout(() => this.cancel(task, new TaskInterrupted('timeout')), task.timeoutMs)
    this.emit('started', task)
    task.execution = Promise.resolve().then(async () => {
      let deliver: (() => void) | undefined, failure: Error | undefined
      try { assertCurrent(); deliver = await task.execute(controller.signal, assertCurrent); assertCurrent() }
      catch (error) { failure = errorOf(error) }
      // 执行器确认退出后才归还资源、唤醒同键/lane 与交付结果。
      if (timer) clearTimeout(timer)
      this.running.delete(task); this.dispatch.release(task); task.controller = undefined
      if (task.stop && (task.stop.kind === 'preempt' || task.stop.kind === 'pause') && task.demands.size && !this.closed) {
        task.restartCostMs += performance.now() - task.startedAt
        task.state = 'queued'; task.queuedAt = performance.now(); task.stop = undefined
        this.updateDemand(task); this.emit('resumed', task)
      } else if (failure || task.stop) {
        this.finish(task, task.stop && task.stop.kind !== 'timeout' ? 'cancelled' : 'failed', task.stop ?? failure!)
      } else { deliver!(); this.finish(task, 'succeeded') }
      this.wake()
    })
  }
  private interrupt(task: Task, reason: TaskInterrupted): void {
    if (task.state !== 'running') return
    task.state = 'stopping'; task.stop = reason
    task.controller!.abort(reason); this.emit('interrupt', task)
  }
  private cancel(task: Task, reason: TaskInterrupted): void {
    if (terminal(task.state)) return
    for (const demand of [...task.demands.values()]) { demand.cleanup(); demand.reject(reason); task.demands.remove(demand.id) }
    task.protectedDemands = 0; this.unindexEviction(task)
    if (task.state === 'queued') this.finish(task, reason.kind === 'timeout' ? 'failed' : 'cancelled', reason)
    else if (task.state === 'running') this.interrupt(task, reason)
    else { task.stop = reason; this.emit('interrupt', task) }
    this.wake()
  }
  private finish(task: Task, state: 'succeeded' | 'failed' | 'cancelled', error?: Error): void {
    task.state = state
    this.dispatch.remove(task); this.unindexEviction(task)
    for (const demand of [...task.demands.values()]) {
      demand.cleanup(); if (error) demand.reject(error); task.demands.remove(demand.id)
    }
    this.emit(state, task, error); this.tasks.delete(task.id); task.remove()
    if (!this.tasks.size) for (const notify of [...this.idleWaiters]) notify()
  }

  private waitIdle(isIdle: () => boolean, waiters: Set<() => void>, options: IdleOptions = {}): Promise<void> {
    options.signal?.throwIfAborted()
    if (options.timeoutMs !== undefined) positive(options.timeoutMs, 'idle timeout')
    if (isIdle()) return Promise.resolve()
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined
      const cleanup = (): void => { waiters.delete(done); options.signal?.removeEventListener('abort', abort); if (timer) clearTimeout(timer) }
      const done = (): void => { if (isIdle()) { cleanup(); resolve() } }
      const abort = (): void => { cleanup(); reject(options.signal!.reason) }
      waiters.add(done); options.signal?.addEventListener('abort', abort, { once: true })
      if (options.timeoutMs !== undefined) timer = setTimeout(() => { cleanup(); reject(new TaskInterrupted('timeout', 'Idle wait timed out')) }, options.timeoutMs)
    })
  }
  onIdle(options?: IdleOptions): Promise<void> { return this.waitIdle(() => this.tasks.size === 0, this.idleWaiters, options) }
  /** 停止接单，允许现有工作完成；原先暂停时继续保持暂停。 */
  drain(options?: IdleOptions): Promise<void> {
    this.accepting = false
    return this.onIdle(options)
  }
  private accepting = true
  pause(): void {
    this.paused = true
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    for (const task of this.running) if (task.mode !== 'atomic') this.interrupt(task, new TaskInterrupted('pause'))
  }
  resume(): void {
    if (this.closed) throw new Error('Scheduler is closed')
    this.paused = false; this.dispatch.retryBlocked(); this.wake()
  }
  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true; this.accepting = false
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined }
    for (const task of [...this.tasks.values()]) this.cancel(task, new TaskInterrupted('close'))
    this.closing = Promise.all([...this.running].map(task => task.execution)).then(() => { this.registrations.clear(); this.listeners.clear() })
    return this.closing
  }
  subscribe(observer: TaskObserver): () => void {
    if (this.closed) throw new Error('Scheduler is closed')
    this.listeners.set(observer, { after: this.sequence })
    return () => { this.listeners.delete(observer) }
  }
  snapshot() {
    const dispatch = this.dispatch.snapshot()
    const history = this.history.length < this.historyLimit ? this.history
      : [...this.history.slice(this.historyCursor), ...this.history.slice(0, this.historyCursor)]
    return { paused: this.paused, closed: this.closed, accepting: this.accepting,
      resources: { ...this.limits }, ...dispatch, tasks: [...this.tasks.values()].map(task => this.describe(task)), history: structuredClone(history) }
  }
  private describe(task: Task): TaskSnapshot {
    return { id: task.id, type: task.type, key: task.key, state: task.state, mode: task.mode,
      ring: task.ring, priority: task.priority, lane: task.lane, segment: task.segment,
      demandCount: task.demands.size, resources: { ...task.resources }, attempt: task.attempt, createdAt: task.createdAt,
      waitedMs: task.waitedMs + (task.state === 'queued' ? performance.now() - task.queuedAt : 0),
      ...(task.stop ? { stop: task.stop.kind } : {}) }
  }
  private emit(action: TaskEvent['action'], task: Task, error?: Error): void {
    task.changed()
    const snapshot = Object.freeze({ ...this.describe(task), resources: Object.freeze({ ...task.resources }) })
    const event: TaskEvent = Object.freeze({ sequence: ++this.sequence, time: Date.now(), action, task: snapshot,
      ...(error ? { error: errorDetails(error) } : {}) })
    if (this.history.length < this.historyLimit) this.history.push(event)
    else { this.history[this.historyCursor] = event; this.historyCursor = (this.historyCursor + 1) % this.historyLimit }
    if (!this.listeners.size || this.notifying) return
    this.notifying = true
    queueMicrotask(() => {
      this.notifying = false
      // 通知与诊断共用环形历史，单次同步大批提交也不会堆积无界事件副本。
      const events = this.history.length < this.historyLimit ? [...this.history]
        : [...this.history.slice(this.historyCursor), ...this.history.slice(0, this.historyCursor)]
      const first = events[0]?.sequence
      if (first === undefined) return
      for (const [observer, cursor] of this.listeners) {
        if (cursor.after < first - 1) {
          const gap = { fromSequence: cursor.after + 1, toSequence: first - 1 }
          cursor.after = first - 1
          try { observer.gap(gap) } catch (error) { console.error('[task-scheduler] gap observer failed', error) }
        }
        for (const entry of events) {
          if (!this.listeners.has(observer)) break
          if (entry.sequence <= cursor.after) continue
          cursor.after = entry.sequence
          try { observer.event(entry) } catch (error) { console.error('[task-scheduler] observer failed', error) }
        }
      }
    })
  }
}
