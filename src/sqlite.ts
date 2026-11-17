import { randomUUID } from 'node:crypto'
import type { DatabaseSync, StatementSync } from 'node:sqlite'
import { QueueVersionConflict } from './types.js'
import type { SchedulingDemand } from './types.js'

export type StoredTaskState = 'pending' | 'claimed' | 'succeeded' | 'failed' | 'cancelled'
export interface StoredTask<I> {
  key: string
  input: I
  demand: SchedulingDemand
  revision: number
  sequence: number
  state: StoredTaskState
  owner: string | null
  token: string | null
  error: string | null
}
export interface StoredTaskClaim<I> extends StoredTask<I> { state: 'claimed'; owner: string; token: string }
export interface PersistentSubmission<I> { key: string; input: I; demand: SchedulingDemand }
export interface PersistentCodec<I> {
  /** 格式版本必须由应用明确声明；旧格式不猜字段或补默认值。 */
  version: string
  encode(input: I): string
  decode(json: string): I
}
export interface SqliteTaskStoreOptions<I> {
  database: DatabaseSync
  namespace: string
  codec: PersistentCodec<I>
}
type Row = Record<string, unknown>
const text = (row: Row, name: string): string => {
  const value = row[name]
  if (typeof value !== 'string') throw new Error(`Invalid persisted task field: ${name}`)
  return value
}
const nullableText = (row: Row, name: string): string | null => row[name] === null ? null : text(row, name)
const integer = (value: unknown, name: string): number => {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error(`Invalid ${name}`)
  return value
}

/**
 * 可选 SQLite 持久收件箱；DatabaseSync 的所有权仍在调用方。
 * 等待候选不预先领取执行资源，执行器实际获得额度后再 claim。
 * 领取、结果写入及任务结算可在同一连接的一个同步事务中完成。
 */
export class SqliteTaskStore<I> {
  private readonly db: DatabaseSync
  private readonly namespace: string
  private readonly codec: PersistentCodec<I>
  private readonly statements = new Map<string, StatementSync>()
  private mutating = false
  private decoding = false

  constructor(options: SqliteTaskStoreOptions<I>) {
    if (!options.namespace || !options.codec.version) throw new Error('Persistent namespace and codec version are required')
    this.db = options.database; this.namespace = options.namespace
    // 固定格式契约，避免调用方后来修改 codec 对象却继续写入旧版本命名空间。
    this.codec = Object.freeze({ version: options.codec.version,
      encode: options.codec.encode.bind(options.codec), decode: options.codec.decode.bind(options.codec) })
    // 不修改连接的 journal/同步策略：调用方可以与自己的业务表共用事务。
    this.transaction(() => {
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS scheduler_namespaces (
          namespace TEXT PRIMARY KEY, schema_version INTEGER NOT NULL,
          codec_version TEXT NOT NULL, version INTEGER NOT NULL, sequence INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS scheduler_tasks (
          namespace TEXT NOT NULL, key TEXT NOT NULL, payload TEXT NOT NULL,
          ring TEXT NOT NULL, priority REAL NOT NULL, revision INTEGER NOT NULL,
          sequence INTEGER NOT NULL, state TEXT NOT NULL
            CHECK(state IN ('pending','claimed','succeeded','failed','cancelled')),
          owner TEXT, token TEXT, error TEXT,
          PRIMARY KEY(namespace,key),
          CHECK ((state='claimed' AND owner IS NOT NULL AND token IS NOT NULL)
            OR (state<>'claimed' AND owner IS NULL AND token IS NULL))
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS scheduler_pending ON scheduler_tasks(namespace,state,sequence);
        CREATE INDEX IF NOT EXISTS scheduler_owner ON scheduler_tasks(namespace,owner,state);
      `)
      this.statement('INSERT OR IGNORE INTO scheduler_namespaces VALUES(?,1,?,0,0)').run(this.namespace, this.codec.version)
      const meta = this.meta()
      if (meta.schema_version !== 1 || meta.codec_version !== this.codec.version) {
        throw new Error(`Persistent queue format mismatch: schema=${String(meta.schema_version)}, codec=${String(meta.codec_version)}`)
      }
    })
  }

  private statement(sql: string): StatementSync {
    let statement = this.statements.get(sql)
    if (!statement) { statement = this.db.prepare(sql); this.statements.set(sql, statement) }
    return statement
  }
  private meta(): Row {
    const row = this.statement('SELECT * FROM scheduler_namespaces WHERE namespace=?').get(this.namespace)
    if (!row) throw new Error(`Missing persistent queue namespace: ${this.namespace}`)
    return row
  }
  get version(): number { return integer(this.meta().version, 'queue version') }
  private transaction<T>(body: () => T): T {
    if (this.mutating || this.decoding) throw new Error('Reentrant persistent queue mutation is not allowed')
    // 不接管调用方已打开的事务：BEGIN 失败发生在 try 外，不会误回滚业务事务。
    this.db.exec('BEGIN IMMEDIATE')
    this.mutating = true
    try {
      const result = body()
      if (result !== null && (typeof result === 'object' || typeof result === 'function') && 'then' in result) {
        throw new Error('Persistent transaction callbacks must be synchronous')
      }
      this.db.exec('COMMIT')
      return result
    } catch (error) {
      try { this.db.exec('ROLLBACK') }
      catch (rollback) { throw new AggregateError([error, rollback], 'Persistent queue transaction and rollback failed') }
      throw error
    } finally { this.mutating = false }
  }
  private mutate<T>(expected: number | undefined, body: () => { value: T; changed: boolean }): T {
    return this.transaction(() => {
      const before = this.version
      if (expected !== undefined && before !== expected) throw new QueueVersionConflict(expected, before)
      const { value, changed } = body()
      if (changed) {
        if (!Number.isSafeInteger(before + 1)) throw new Error('Persistent queue version exhausted')
        this.statement('UPDATE scheduler_namespaces SET version=version+1 WHERE namespace=?').run(this.namespace)
      }
      return value
    })
  }
  private validateDemand(demand: SchedulingDemand): void {
    if (!demand.ring || !Number.isFinite(demand.priority) || demand.priority < -100 || demand.priority > 100) {
      throw new Error('Persistent task requires a ring and priority between -100 and 100')
    }
  }
  private nextRevision(row: Row): number {
    const revision = integer(row.revision, 'task revision') + 1
    if (!Number.isSafeInteger(revision)) throw new Error('Persistent task revision exhausted')
    return revision
  }
  private decodePayload(payload: string): I {
    if (this.decoding) throw new Error('Reentrant persistent codec decoding is not allowed')
    this.decoding = true
    try { return this.codec.decode(payload) }
    finally { this.decoding = false }
  }
  private encode(input: I): string {
    const payload = this.codec.encode(input)
    if (typeof payload !== 'string') throw new Error('Persistent codec must return a string')
    this.decodePayload(payload) // 写入前验证同一格式确实能读回，不补字段。
    return payload
  }
  private decode(row: Row): StoredTask<I> {
    const state = text(row, 'state')
    if (state !== 'pending' && state !== 'claimed' && state !== 'succeeded' && state !== 'failed' && state !== 'cancelled') throw new Error('Invalid persisted task state')
    const priority = row.priority
    if (typeof priority !== 'number') throw new Error('Invalid persisted task priority')
    const demand = { ring: text(row, 'ring'), priority }
    this.validateDemand(demand)
    const owner = nullableText(row, 'owner'), token = nullableText(row, 'token')
    if (state === 'claimed' ? !owner || !token : owner !== null || token !== null) throw new Error('Invalid persisted task ownership')
    return { key: text(row, 'key'), input: this.decodePayload(text(row, 'payload')), demand,
      revision: integer(row.revision, 'task revision'), sequence: integer(row.sequence, 'task sequence'),
      state, owner, token, error: nullableText(row, 'error') }
  }
  private row(key: string): Row | undefined {
    return this.statement('SELECT * FROM scheduler_tasks WHERE namespace=? AND key=?').get(this.namespace, key)
  }
  get(key: string): StoredTask<I> | undefined { const row = this.row(key); return row ? this.decode(row) : undefined }

  /** 整批发布；重复身份只允许完全一致的有效载荷，调度需求变更用 promote。 */
  submitBatch(submissions: readonly PersistentSubmission<I>[], expectedVersion?: number): StoredTask<I>[] {
    // 先固定整批身份/需求，再在受保护的事务内调用 codec。codec 不得重入队列。
    const snapshot = submissions.map(item => ({ key: item.key, input: item.input,
      demand: { ring: item.demand.ring, priority: item.demand.priority } }))
    return this.mutate(expectedVersion, () => {
      const prepared = snapshot.map(item => {
        if (!item.key) throw new Error('Persistent task key must be nonempty')
        this.validateDemand(item.demand)
        return { ...item, payload: this.encode(item.input) }
      })
      let sequence = integer(this.meta().sequence, 'task sequence'), changed = false
      const result: StoredTask<I>[] = []
      for (const item of prepared) {
        let row = this.row(item.key)
        if (row) {
          if (row.payload !== item.payload || row.ring !== item.demand.ring || row.priority !== item.demand.priority) {
            throw new Error(`Conflicting persistent task: ${item.key}`)
          }
        } else {
          sequence++
          if (!Number.isSafeInteger(sequence)) throw new Error('Persistent task sequence exhausted')
          this.statement("INSERT INTO scheduler_tasks VALUES(?,?,?,?,?,1,?,'pending',NULL,NULL,NULL)")
            .run(this.namespace, item.key, item.payload, item.demand.ring, item.demand.priority, sequence)
          row = this.row(item.key)!
          changed = true
        }
        result.push(this.decode(row))
      }
      if (changed) this.statement('UPDATE scheduler_namespaces SET sequence=? WHERE namespace=?').run(sequence, this.namespace)
      return { value: result, changed }
    })
  }
  /** 等待载荷的版本化替换；正在执行或已结算的任务必须使用新的业务身份。 */
  updatePending(key: string, revision: number, input: I, expectedVersion: number): StoredTask<I> {
    return this.mutate(expectedVersion, () => {
      const row = this.row(key)
      if (!row || row.state !== 'pending' || row.revision !== revision) throw new Error(`Pending task revision conflict: ${key}`)
      const next = this.nextRevision(row), payload = this.encode(input)
      this.statement('UPDATE scheduler_tasks SET payload=?,revision=? WHERE namespace=? AND key=?').run(payload, next, this.namespace, key)
      return { value: this.decode(this.row(key)!), changed: true }
    })
  }
  promote(key: string, demand: SchedulingDemand, expectedVersion: number): StoredTask<I> {
    this.validateDemand(demand)
    return this.mutate(expectedVersion, () => {
      const row = this.row(key)
      if (!row || row.state !== 'pending') throw new Error(`Task is not pending: ${key}`)
      const changed = row.ring !== demand.ring || row.priority !== demand.priority
      if (changed) this.statement('UPDATE scheduler_tasks SET ring=?,priority=?,revision=? WHERE namespace=? AND key=?')
        .run(demand.ring, demand.priority, this.nextRevision(row), this.namespace, key)
      return { value: this.decode(this.row(key)!), changed }
    })
  }
  cancelPending(keys: readonly string[], expectedVersion: number): void {
    this.mutate(expectedVersion, () => {
      const unique = new Set(keys)
      for (const key of unique) {
        const row = this.row(key)
        if (!row || row.state !== 'pending') throw new Error(`Task is not pending: ${key}`)
        this.statement("UPDATE scheduler_tasks SET state='cancelled',revision=? WHERE namespace=? AND key=?")
          .run(this.nextRevision(row), this.namespace, key)
      }
      return { value: undefined, changed: unique.size > 0 }
    })
  }
  pending(limit: number): StoredTask<I>[] {
    if (integer(limit, 'pending limit') < 1) throw new Error('Pending limit must be positive')
    return this.statement("SELECT * FROM scheduler_tasks WHERE namespace=? AND state='pending' ORDER BY sequence LIMIT ?")
      .all(this.namespace, limit).map(row => this.decode(row))
  }
  /** 候选 revision 防止枚举之后 payload 被改写；整批领取失败则事务回滚。 */
  claim(candidates: readonly { key: string; revision: number }[], owner: string): StoredTaskClaim<I>[] {
    if (!owner) throw new Error('Claim owner must be nonempty')
    const snapshot = candidates.map(({ key, revision }) => ({ key, revision }))
    return this.mutate(undefined, () => {
      const keys = new Set<string>(), claims: StoredTaskClaim<I>[] = []
      for (const candidate of snapshot) {
        if (keys.has(candidate.key)) throw new Error(`Duplicate claim key: ${candidate.key}`)
        keys.add(candidate.key)
        const row = this.row(candidate.key)
        if (!row || row.state !== 'pending' || row.revision !== candidate.revision) throw new Error(`Pending task revision conflict: ${candidate.key}`)
        const token = randomUUID()
        this.statement("UPDATE scheduler_tasks SET state='claimed',owner=?,token=?,revision=? WHERE namespace=? AND key=?")
          .run(owner, token, this.nextRevision(row), this.namespace, candidate.key)
        const task = this.decode(this.row(candidate.key)!)
        claims.push({ ...task, state: 'claimed', owner, token })
      }
      return { value: claims, changed: claims.length > 0 }
    })
  }
  /** 凭据不再有效时直接报错，不允许旧执行静默覆盖新世代。 */
  settle(claim: StoredTaskClaim<I>, outcome: { state: 'succeeded' } | { state: 'failed'; error: string },
    commit?: (database: DatabaseSync, input: I) => void): void {
    const credentials = { key: claim.key, owner: claim.owner, token: claim.token, revision: claim.revision }
    const state = outcome.state, error = outcome.state === 'failed' ? outcome.error : null
    this.mutate(undefined, () => {
      const current = this.checkClaim(credentials)
      const next = this.nextRevision({ revision: current.revision })
      const result: unknown = commit?.(this.db, current.input)
      if (result !== null && (typeof result === 'object' || typeof result === 'function') && 'then' in result) {
        throw new Error('Result commit must be synchronous')
      }
      // 业务回调只写业务表；回调之后仍检查领取凭据，避免同连接意外改写任务。
      const changes = this.statement(`UPDATE scheduler_tasks SET state=?,error=?,owner=NULL,token=NULL,revision=?
        WHERE namespace=? AND key=? AND state='claimed' AND owner=? AND token=? AND revision=?`)
        .run(state, error, next, this.namespace, credentials.key, credentials.owner, credentials.token, credentials.revision).changes
      if (changes !== 1) throw new Error(`Stale persistent claim: ${credentials.key}`)
      return { value: undefined, changed: true }
    })
  }
  release(claim: StoredTaskClaim<I>): void {
    const credentials = { key: claim.key, owner: claim.owner, token: claim.token, revision: claim.revision }
    this.mutate(undefined, () => {
      const current = this.checkClaim(credentials)
      this.statement("UPDATE scheduler_tasks SET state='pending',owner=NULL,token=NULL,revision=? WHERE namespace=? AND key=?")
        .run(this.nextRevision({ revision: current.revision }), this.namespace, credentials.key)
      return { value: undefined, changed: true }
    })
  }
  private checkClaim(claim: Pick<StoredTaskClaim<I>, 'key' | 'owner' | 'token' | 'revision'>): StoredTask<I> {
    const current = this.get(claim.key)
    if (!current || current.state !== 'claimed' || current.owner !== claim.owner || current.token !== claim.token || current.revision !== claim.revision) {
      throw new Error(`Stale persistent claim: ${claim.key}`)
    }
    return current
  }
  /** 调用方先确认旧拥有者已退出。不会根据心跳超时猜测原子外部副作用是否发生。 */
  recover(owner: string, policy: { kind: 'retry' } | { kind: 'fail'; error: string }): number {
    if (!owner) throw new Error('Recovery requires an explicit prior owner')
    return this.mutate(undefined, () => {
      const maximum = this.statement("SELECT MAX(revision) revision FROM scheduler_tasks WHERE namespace=? AND state='claimed' AND owner=?")
        .get(this.namespace, owner)!
      if (maximum.revision !== null) this.nextRevision(maximum)
      const changes = Number(this.statement("UPDATE scheduler_tasks SET state=?,error=?,owner=NULL,token=NULL,revision=revision+1 WHERE namespace=? AND state='claimed' AND owner=?")
        .run(policy.kind === 'retry' ? 'pending' : 'failed', policy.kind === 'fail' ? policy.error : null, this.namespace, owner).changes)
      return { value: changes, changed: changes > 0 }
    })
  }
  /** 删除已结算任务；活跃记录永不被历史清理移除。 */
  prune(retain: number): number {
    integer(retain, 'retained task count')
    return this.mutate(undefined, () => {
      const changes = Number(this.statement(`DELETE FROM scheduler_tasks WHERE namespace=? AND key IN (
        SELECT key FROM scheduler_tasks WHERE namespace=? AND state IN ('succeeded','failed','cancelled')
        ORDER BY sequence DESC LIMIT -1 OFFSET ?
      )`).run(this.namespace, this.namespace, retain).changes)
      return { value: changes, changed: changes > 0 }
    })
  }
}
