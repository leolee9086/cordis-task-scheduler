import { IndexedHeap } from './heap.js'
import type { ResourceDemand } from './types.js'

/** 环间顺序与环内排序是两套规则；数值优先级不会跨环比较。 */
export interface RingPolicy {
  name: string
  /** 越小越先分派；同 rank 按声明顺序轮转。 */
  rank: number
  order?: 'priority' | 'fifo' | 'lifo'
  capacity?: number
  concurrency?: number
  resources?: ResourceDemand
  agingMs?: number
  /** 只允许中断这里列出的环；同环抢占由 preemptWithin 控制。 */
  preempts?: readonly string[]
  preemptWithin?: boolean
  /** 持续等待期间最多让过多少次其他环分派。到期可中断可恢复任务，取得一个 quantum 的服务机会。 */
  maxBypass?: number
}

export interface DispatchTask {
  id: number
  type: string
  key: string
  ring: string
  /** 容量按首次准入环计费；有效需求提权不会偷换积压容量归属。 */
  admissionRing: string
  priority: number
  order: 'fifo' | 'lifo'
  mode: 'restartable' | 'atomic' | 'orchestrator'
  resources: ResourceDemand
  /** 入队时复用已规范化的资源向量，避免提权/重试反复排序与序列化。 */
  resourceProfile: string
  lane?: string
  score: number
  startedAt: number
  dispatchWeight: number
  protectedUntil: number
  restartCostMs: number
  state: string
}
interface Group<T extends DispatchTask> {
  id: number
  profile: string
  ring: Ring<T>
  tasks: IndexedHeap<T>
  head?: T
  blocked?: string
}
interface Ring<T extends DispatchTask> {
  policy: RingPolicy
  index: number
  groups: Map<string, Group<T>>
  ready: IndexedHeap<Group<T>>
  count: number
  queued: number
  executions: number
  used: Map<string, number>
  waitingSince: number
  lastServed: number
}
interface Allocation<T extends DispatchTask> { task: T; ring: Ring<T> }

const integer = (value: number, minimum: number, name: string): number => {
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`Invalid ${name}: ${value}`)
  return value
}

/**
 * 每个环内按资源向量分组，每组只向候选堆贡献一个头部。
 * 资源不足时挂起整个组；同键/lane 阻塞只挂起对应任务，避免堵住无关任务。
 * 入队、撤销与提权只修复所涉堆；运行额度以增量计数维护。
 */
export class DispatchIndex<T extends DispatchTask> {
  private readonly rings = new Map<string, Ring<T>>()
  private readonly groupsByTask = new Map<number, Group<T>>()
  private readonly parked = new Map<number, { task: T; reason: string }>()
  private readonly taskWaiters = new Map<string, Set<T>>()
  private readonly groupWaiters = new Map<string, Set<Group<T>>>()
  private readonly allocations = new Map<number, Allocation<T>>()
  private readonly keys = new Map<string, Set<string>>()
  private readonly lanes = new Set<string>()
  private readonly used = new Map<string, number>()
  private executions = 0
  private nextGroup = 0
  private dispatches = 0

  constructor(
    policies: readonly RingPolicy[],
    private readonly limits: ResourceDemand,
    private readonly concurrency: number,
    private readonly agingMs: number,
    private readonly quantumMs: number,
  ) {
    if (!policies.length) throw new Error('At least one scheduling ring is required')
    for (const [index, value] of policies.entries()) {
      if (!value.name || this.rings.has(value.name)) throw new Error(`Duplicate or empty ring: ${value.name}`)
      if (!Number.isFinite(value.rank)) throw new Error(`Invalid ring rank: ${value.name}`)
      if (value.capacity !== undefined) integer(value.capacity, 1, 'ring capacity')
      if (value.concurrency !== undefined) integer(value.concurrency, 1, 'ring concurrency')
      if (value.maxBypass !== undefined) integer(value.maxBypass, 1, 'maxBypass')
      if (value.agingMs !== undefined) integer(value.agingMs, 1, 'ring agingMs')
      for (const [name, amount] of Object.entries(value.resources ?? {})) {
        integer(amount, 1, 'ring resource')
        if (limits[name] === undefined || amount > limits[name]!) throw new Error(`Unavailable ring resource: ${name}`)
      }
      const policy = { ...value, resources: Object.freeze({ ...value.resources }), preempts: [...value.preempts ?? []] }
      const ring: Ring<T> = {
        policy, index, groups: new Map(), count: 0, queued: 0, executions: 0,
        used: new Map(), waitingSince: 0, lastServed: 0,
        ready: new IndexedHeap((a, b) => this.compareInRing(a.head!, b.head!, policy)),
      }
      this.rings.set(value.name, ring)
    }
    for (const ring of this.rings.values()) for (const target of ring.policy.preempts ?? []) this.getRing(target)
  }

  policy(name: string): Readonly<RingPolicy> { return this.getRing(name).policy }
  aging(name: string): number { return this.getRing(name).policy.agingMs ?? this.agingMs }
  count(name: string): number { return this.getRing(name).count }
  /** 需求聚合只用静态环级顺序，公平轮转不改变任务所属环。 */
  compareDemand(a: { ring: string; priority: number }, b: { ring: string; priority: number }): number {
    const first = this.getRing(a.ring), second = this.getRing(b.ring)
    return first.policy.rank - second.policy.rank || first.index - second.index || b.priority - a.priority
  }
  compareEviction(a: T, b: T): number { return -this.compareDemand(a, b) || a.id - b.id }
  add(task: T): void { this.getRing(task.admissionRing).count++ }
  remove(task: T): void { this.dequeue(task); this.getRing(task.admissionRing).count-- }
  move(task: T, ring: string): void {
    this.dequeue(task)
    task.ring = ring
  }

  enqueue(task: T): void {
    const ring = this.getRing(task.ring), waitingSince = ring.waitingSince
    const wasWaiting = ring.queued > 0
    this.dequeue(task)
    // 规范化资源顺序后，相同需求共享一组；身份和 lane 不参与资源分组。
    const profile = task.resourceProfile
    let group = ring.groups.get(profile)
    if (!group) {
      group = { id: ++this.nextGroup, profile, ring, tasks: new IndexedHeap((a, b) => this.compareInRing(a, b, ring.policy)) }
      ring.groups.set(profile, group)
    }
    this.groupsByTask.set(task.id, group)
    ring.queued++
    ring.waitingSince = wasWaiting ? waitingSince : this.dispatches
    group.tasks.upsert(task)
    // 新需求或提权可能允许抢占，应立即重新检查该组。
    if (group.blocked) {
      this.removeWaiter(this.groupWaiters, group.blocked, group)
      group.blocked = undefined
    }
    this.refresh(group)
  }
  dequeue(task: T): void {
    const parked = this.parked.get(task.id)
    if (parked) {
      this.parked.delete(task.id)
      this.removeWaiter(this.taskWaiters, parked.reason, task)
      this.getRing(task.ring).queued--
    }
    const group = this.groupsByTask.get(task.id)
    if (!group) return
    this.groupsByTask.delete(task.id)
    group.tasks.remove(task.id)
    group.ring.queued--
    this.refresh(group)
  }
  private refresh(group: Group<T>): void {
    const head = group.tasks.peek()
    if (!head) {
      // 先从候选堆移除，再清空 head，堆修复期间比较器始终有有效任务。
      group.ring.ready.remove(group.id)
      group.head = undefined
      group.ring.groups.delete(group.profile)
      if (group.blocked) {
        const waiters = this.groupWaiters.get(group.blocked)
        waiters?.delete(group)
        if (!waiters?.size) this.groupWaiters.delete(group.blocked)
        group.blocked = undefined
      }
      return
    }
    group.head = head
    if (!group.blocked) group.ring.ready.upsert(group)
    else group.ring.ready.remove(group.id)
  }
  private removeWaiter<V>(index: Map<string, Set<V>>, reason: string, value: V): void {
    const waiters = index.get(reason)
    if (!waiters) return
    waiters.delete(value)
    if (!waiters.size) index.delete(reason)
  }
  private getRing(name: string): Ring<T> {
    const ring = this.rings.get(name)
    if (!ring) throw new Error(`Unknown scheduling ring: ${name}`)
    return ring
  }
  private compareInRing(a: T, b: T, policy: RingPolicy): number {
    if (policy.order === 'fifo') return a.id - b.id
    if (policy.order === 'lifo') return b.id - a.id
    return b.score - a.score || (a.order === 'lifo' ? -a.id : a.id) - (b.order === 'lifo' ? -b.id : b.id)
  }
  private overdue(ring: Ring<T>): boolean {
    return ring.queued > 0 && ring.policy.maxBypass !== undefined
      && this.dispatches - ring.waitingSince >= ring.policy.maxBypass
  }
  next(): { task: T; fair: boolean } | undefined {
    let selected: Ring<T> | undefined
    for (const ring of this.rings.values()) {
      if (!ring.ready.size) continue
      if (!selected) { selected = ring; continue }
      const fair = this.overdue(ring), previousFair = this.overdue(selected)
      if ((fair && !previousFair)
        || (fair && previousFair && ring.waitingSince < selected.waitingSince)
        || (!fair && !previousFair && (ring.policy.rank < selected.policy.rank
          || (ring.policy.rank === selected.policy.rank && ring.lastServed < selected.lastServed)))) selected = ring
    }
    return selected ? { task: selected.ready.peek()!.head!, fair: this.overdue(selected) } : undefined
  }

  blocker(task: T): string | undefined {
    if (this.keys.get(task.type)?.has(task.key)) return `key:${JSON.stringify([task.type, task.key])}`
    if (task.lane !== undefined && this.lanes.has(task.lane)) return `lane:${task.lane}`
    if (task.mode === 'orchestrator') return undefined
    const ring = this.getRing(task.ring)
    if (this.executions >= this.concurrency) return 'concurrency'
    if (ring.executions >= (ring.policy.concurrency ?? this.concurrency)) return `ring:${ring.policy.name}`
    for (const [name, amount] of Object.entries(task.resources)) {
      if ((this.used.get(name) ?? 0) + amount > this.limits[name]!) return `resource:${name}`
      if ((ring.used.get(name) ?? 0) + amount > (ring.policy.resources?.[name] ?? this.limits[name]!)) return `ringResource:${JSON.stringify([ring.policy.name, name])}`
    }
    return undefined
  }
  block(task: T, reason: string): void {
    if (reason.startsWith('key:') || reason.startsWith('lane:') || reason.startsWith('segment:')) {
      this.dequeue(task)
      this.getRing(task.ring).queued++
      this.parked.set(task.id, { task, reason })
      let waiters = this.taskWaiters.get(reason)
      if (!waiters) this.taskWaiters.set(reason, waiters = new Set())
      waiters.add(task)
      return
    }
    const group = this.groupsByTask.get(task.id)!
    group.blocked = reason
    let waiters = this.groupWaiters.get(reason)
    if (!waiters) this.groupWaiters.set(reason, waiters = new Set())
    waiters.add(group)
    this.refresh(group)
  }
  wake(reason: string): void {
    const tasks = this.taskWaiters.get(reason)
    if (tasks) {
      this.taskWaiters.delete(reason)
      for (const task of tasks) this.enqueue(task)
    }
    const groups = this.groupWaiters.get(reason)
    if (groups) {
      this.groupWaiters.delete(reason)
      for (const group of groups) { group.blocked = undefined; this.refresh(group) }
    }
  }
  /** 时间片到期只重新考虑资源组头部，不遍历组内等待任务。 */
  retryBlocked(): void {
    for (const reason of [...this.groupWaiters.keys()]) this.wake(reason)
  }

  /**
   * 只在候选确实被阻塞时检查运行集合。假设释放量单次累加；
   * stopping 已占用的额度仍计入候选释放量，避免重复中断更多执行。
   */
  victims(task: T, fair: boolean, now: number): T[] {
    if (task.mode === 'orchestrator') return []
    if (this.keys.get(task.type)?.has(task.key) || (task.lane !== undefined && this.lanes.has(task.lane))) return []
    const ring = this.getRing(task.ring), candidates: Allocation<T>[] = []
    for (const allocation of this.allocations.values()) {
      const other = allocation.task
      if (other.mode === 'orchestrator') continue
      if (other.state === 'stopping') { candidates.push(allocation); continue }
      if (other.mode !== 'restartable' || now - other.startedAt < this.quantumMs || now < other.protectedUntil) continue
      const eligible = fair && other.ring !== task.ring
        || (other.ring !== task.ring && ring.policy.preempts?.includes(other.ring))
        || (other.ring === task.ring && ring.policy.preemptWithin !== false
          && (ring.policy.order ?? 'priority') === 'priority'
          && task.score + now / this.aging(task.ring) > other.dispatchWeight
          && (task.priority > other.priority || task.score + now / this.aging(task.ring) >= other.dispatchWeight + 1))
      if (eligible) candidates.push(allocation)
    }
    candidates.sort((a, b) => Number(b.task.state === 'stopping') - Number(a.task.state === 'stopping')
      || this.compareEviction(a.task, b.task))
    let executions = this.executions, ringExecutions = ring.executions
    const used = new Map(this.used), ringUsed = new Map(ring.used), selected: T[] = []
    const fits = (): boolean => executions < this.concurrency
      && ringExecutions < (ring.policy.concurrency ?? this.concurrency)
      && Object.entries(task.resources).every(([name, amount]) => (used.get(name) ?? 0) + amount <= this.limits[name]!
        && (ringUsed.get(name) ?? 0) + amount <= (ring.policy.resources?.[name] ?? this.limits[name]!))
    for (const allocation of candidates) {
      const other = allocation.task
      const conflict = executions >= this.concurrency
        || (allocation.ring === ring && ringExecutions >= (ring.policy.concurrency ?? this.concurrency))
        || Object.entries(task.resources).some(([name, amount]) => (other.resources[name] ?? 0) > 0
          && ((used.get(name) ?? 0) + amount > this.limits[name]!
            || (allocation.ring === ring && (ringUsed.get(name) ?? 0) + amount > (ring.policy.resources?.[name] ?? this.limits[name]!))))
      if (!conflict) continue
      selected.push(other); executions--
      if (allocation.ring === ring) ringExecutions--
      for (const [name, amount] of Object.entries(other.resources)) {
        used.set(name, (used.get(name) ?? 0) - amount)
        if (allocation.ring === ring) ringUsed.set(name, (ringUsed.get(name) ?? 0) - amount)
      }
      if (fits()) return selected
    }
    return []
  }

  acquire(task: T, fair: boolean, now: number): void {
    this.dequeue(task)
    const ring = this.getRing(task.ring)
    this.allocations.set(task.id, { task, ring })
    let keys = this.keys.get(task.type)
    if (!keys) this.keys.set(task.type, keys = new Set())
    keys.add(task.key)
    if (task.lane !== undefined) this.lanes.add(task.lane)
    if (task.mode !== 'orchestrator') { this.executions++; ring.executions++ }
    for (const [name, amount] of Object.entries(task.resources)) {
      this.used.set(name, (this.used.get(name) ?? 0) + amount)
      ring.used.set(name, (ring.used.get(name) ?? 0) + amount)
    }
    // 未产生检查点的重启会重复此前工作；公平服务窗口至少覆盖已浪费的执行时间。
    // 有限任务因而能逐次取得更长的连续执行机会，而不是永远在固定时间片里从头重做。
    task.protectedUntil = fair ? now + Math.max(this.quantumMs, task.restartCostMs * 2) : now
    ring.lastServed = ++this.dispatches
    ring.waitingSince = this.dispatches
    // 新的分派可能使另一个环的让行预算耗尽。
    for (const candidate of this.rings.values()) if (this.overdue(candidate)) this.retryBlockedRing(candidate)
  }
  private retryBlockedRing(ring: Ring<T>): void {
    for (const group of ring.groups.values()) {
      if (!group.blocked) continue
      this.removeWaiter(this.groupWaiters, group.blocked, group)
      group.blocked = undefined
      this.refresh(group)
    }
  }
  release(task: T): void {
    const allocation = this.allocations.get(task.id)
    if (!allocation) throw new Error(`Missing allocation: ${task.id}`)
    this.allocations.delete(task.id)
    const ring = allocation.ring
    const keys = this.keys.get(task.type)!
    keys.delete(task.key)
    if (!keys.size) this.keys.delete(task.type)
    this.wake(`key:${JSON.stringify([task.type, task.key])}`)
    if (task.lane !== undefined) { this.lanes.delete(task.lane); this.wake(`lane:${task.lane}`) }
    if (task.mode !== 'orchestrator') { this.executions--; ring.executions-- }
    for (const [name, amount] of Object.entries(task.resources)) {
      this.used.set(name, this.used.get(name)! - amount)
      ring.used.set(name, ring.used.get(name)! - amount)
      this.wake(`resource:${name}`)
      this.wake(`ringResource:${JSON.stringify([ring.policy.name, name])}`)
    }
    this.wake('concurrency'); this.wake(`ring:${ring.policy.name}`)
  }
  get hasBlockedGroups(): boolean { return this.groupWaiters.size > 0 }
  snapshot() {
    return { used: Object.fromEntries(this.used), indexes: {
      resourceGroups: [...this.rings.values()].reduce((sum, ring) => sum + ring.groups.size, 0),
      queuedTasks: this.groupsByTask.size, parkedTasks: this.parked.size,
      taskWaitReasons: this.taskWaiters.size, resourceWaitReasons: this.groupWaiters.size,
      allocations: this.allocations.size,
    }, rings: [...this.rings.values()].map(ring => ({
      name: ring.policy.name, tasks: ring.count, queued: ring.queued, running: ring.executions,
      bypasses: ring.queued ? this.dispatches - ring.waitingSince : 0,
    })) }
  }
}
