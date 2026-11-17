import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import { createReadStream } from 'node:fs'
import { readdir, readFile, stat, mkdir, mkdtemp } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { fork } from 'node:child_process'
import { join, resolve } from 'node:path'
import { SqliteTaskStore } from '../dist/sqlite.js'
import type { PersistentCodec } from '../dist/sqlite.js'
import { TaskScheduler, QueueVersionConflict } from '../dist/index.js'

interface Source { path: string; bytes: number; mtimeMs: number }
const directory = process.argv[2]
if (!directory) throw new Error('Pass an existing directory containing real files')
const root = resolve(directory), files: Source[] = []
for (const entry of await readdir(root, { withFileTypes: true })) {
  if (!entry.isFile() || !/\.(exe|dll|pak|dat|bin|html)$/.test(entry.name)) continue
  const path = join(root, entry.name), info = await stat(path)
  files.push({ path, bytes: info.size, mtimeMs: info.mtimeMs })
}
assert(files.length >= 3)
const codec: PersistentCodec<Source> = {
  version: 'file-stat-v1', encode: input => JSON.stringify(input),
  decode: json => {
    const value: unknown = JSON.parse(json)
    if (!value || typeof value !== 'object' || !('path' in value) || typeof value.path !== 'string'
      || !('bytes' in value) || typeof value.bytes !== 'number' || !Number.isSafeInteger(value.bytes)
      || !('mtimeMs' in value) || typeof value.mtimeMs !== 'number' || !Number.isFinite(value.mtimeMs)) {
      throw new Error('Invalid stored file source')
    }
    return { path: value.path, bytes: value.bytes, mtimeMs: value.mtimeMs }
  },
}
// 子进程真实领取后保持连接；父进程终止它并等待 exit，再按明确 owner 恢复。
if (process.argv[3] === '--claim-worker') {
  const file = process.argv[4]
  if (!file || !process.send) throw new Error('Claim worker requires database and IPC')
  const connection = new DatabaseSync(file)
  const workerStore = new SqliteTaskStore({ database: connection, namespace: 'file-digests', codec })
  workerStore.claim(workerStore.pending(1), `process:${process.pid}`)
  process.send('claimed')
  await new Promise<void>(() => { process.on('disconnect', () => process.exit(1)) })
  throw new Error('Claim worker unexpectedly resumed')
}
const output = resolve('.local/persistence-verification')
await mkdir(output, { recursive: true })
const run = await mkdtemp(join(output, 'files-')), databasePath = join(run, 'tasks.sqlite')
let database = new DatabaseSync(databasePath)
const openStore = () => new SqliteTaskStore({ database, namespace: 'file-digests', codec })
const scheduler = new TaskScheduler({ resources: { disk: 1 }, concurrency: 1 })
try {
  database.exec('CREATE TABLE digests (path TEXT PRIMARY KEY, sha256 TEXT NOT NULL)')
  let store = openStore()
  const submitted = store.submitBatch(files.map(input => ({ key: input.path, input, demand: { ring: 'default', priority: 0 } })), 0)
  assert.equal(store.version, 1)
  assert.equal(submitted.length, files.length)
  assert.throws(() => store.promote(files[0]!.path, { ring: 'default', priority: 10 }, 0), QueueVersionConflict)
  const updated = store.updatePending(files[0]!.path, submitted[0]!.revision, files[0]!, store.version)
  assert.equal(updated.revision, submitted[0]!.revision + 1)
  const promoted = store.promote(files[0]!.path, { ring: 'default', priority: 10 }, store.version)
  const previousOwner = 'closed-file-reader'
  const abandoned = store.claim([{ key: promoted.key, revision: promoted.revision }], previousOwner)[0]!
  database.close()
  database = new DatabaseSync(databasePath)
  store = openStore()
  assert.equal(store.get(abandoned.key)!.state, 'claimed')
  assert.equal(store.recover(previousOwner, { kind: 'retry' }), 1)
  assert.throws(() => store.settle(abandoned, { state: 'succeeded' }), /Stale persistent claim/)

  // 同一真实数据库的两个连接读取同一候选，只有第一个可以领取。
  const peerDatabase = new DatabaseSync(databasePath)
  try {
    const peer = new SqliteTaskStore({ database: peerDatabase, namespace: 'file-digests', codec })
    const candidate = peer.pending(1)[0]!, version = peer.version
    const owned = store.claim([candidate], 'primary-connection')[0]!
    assert.throws(() => peer.claim([candidate], 'peer-connection'), /revision conflict/)
    assert.throws(() => peer.promote(candidate.key, candidate.demand, version), QueueVersionConflict)
    assert.equal(peer.get(candidate.key)!.token, owned.token)
    store.release(owned)
  } finally { peerDatabase.close() }

  const worker = fork(new URL(import.meta.url), [root, '--claim-worker', databasePath], {
    execArgv: ['--experimental-strip-types'], stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  })
  const exited = new Promise<void>((resolveExit, reject) => {
    worker.once('error', reject); worker.once('exit', () => resolveExit())
  })
  try {
    await new Promise<void>((resolveReady, reject) => {
      const timeout = setTimeout(() => reject(new Error('Claim worker did not become ready')), 30000)
      worker.once('message', message => {
        clearTimeout(timeout)
        if (message === 'claimed') resolveReady()
        else reject(new Error('Unexpected claim worker message'))
      })
      worker.once('error', error => { clearTimeout(timeout); reject(error) })
      worker.once('exit', () => { clearTimeout(timeout); reject(new Error('Claim worker exited before ready')) })
    })
    const owner = `process:${worker.pid}`
    const claimed = files.map(file => store.get(file.path)!).find(task => task.owner === owner)
    assert(claimed && claimed.state === 'claimed' && claimed.owner && claimed.token)
    const stale = { ...claimed, state: 'claimed' as const, owner: claimed.owner, token: claimed.token }
    assert(worker.kill('SIGKILL'))
    await exited
    assert.equal(store.get(stale.key)!.state, 'claimed')
    assert.equal(store.recover(owner, { kind: 'retry' }), 1)
    assert.throws(() => store.release(stale), /Stale persistent claim/)
  } finally {
    if (worker.exitCode === null && worker.signalCode === null) worker.kill('SIGKILL')
    await exited
  }

  // 已有业务事务不能被队列接管或误回滚。
  const transactionVersion = store.version
  database.exec('BEGIN IMMEDIATE')
  try {
    assert.throws(() => store.prune(0), /transaction/)
    assert.equal(store.version, transactionVersion)
    database.exec('ROLLBACK')
  } catch (error) {
    database.exec('ROLLBACK')
    throw error
  }

  // 让 SQLite 的真实主键约束失败，核验业务写入与队列版本都回滚。
  const candidates = store.pending(files.length)
  const rollbackClaim = store.claim([candidates[0]!], 'file-reader')[0]!
  const before = store.version
  const rollbackDigest = createHash('sha256').update(await readFile(rollbackClaim.input.path)).digest('hex')
  assert.throws(() => store.settle(rollbackClaim, { state: 'succeeded' }, (db, input) => {
    const insert = db.prepare('INSERT INTO digests VALUES(?,?)')
    insert.run(input.path, rollbackDigest); insert.run(input.path, rollbackDigest)
  }), /UNIQUE constraint failed/)
  assert.equal(store.version, before)
  assert.equal(store.get(rollbackClaim.key)!.state, 'claimed')
  assert.equal(database.prepare('SELECT COUNT(*) count FROM digests').get()!.count, 0)
  store.release(rollbackClaim)

  // 实际执行额度领取后再持久 claim；完成文件版本核对后，同事务写摘要和 succeeded。
  const queue = scheduler.register<ReturnType<typeof store.pending>[number], string>({
    name: 'persisted.file.digest', mode: 'restartable', key: task => task.key,
    resources: () => ({ disk: 1 }),
    execute: async (task, execution) => {
      const claim = store.claim([{ key: task.key, revision: task.revision }], 'file-reader')[0]!
      try {
        const hash = createHash('sha256')
        for await (const chunk of createReadStream(claim.input.path, { signal: execution.signal })) hash.update(chunk)
        const info = await stat(claim.input.path)
        assert.equal(info.size, claim.input.bytes); assert.equal(info.mtimeMs, claim.input.mtimeMs)
        execution.assertCurrent()
        const digest = hash.digest('hex')
        store.settle(claim, { state: 'succeeded' }, (db, input) => {
          db.prepare('INSERT INTO digests VALUES(?,?)').run(input.path, digest)
        })
        return digest
      } catch (error) {
        store.release(claim)
        throw error
      }
    },
  })
  const pending = store.pending(files.length)
  const leases = queue.submitBatch(pending.map(input => ({ input, options: input.demand })))
  await Promise.all(leases.map(lease => lease.result))
  assert.equal(store.pending(files.length).length, 0)
  const version = store.version
  database.close(); database = new DatabaseSync(databasePath); store = openStore()
  assert.equal(store.version, version)
  for (const file of files) {
    assert.equal(store.get(file.path)!.state, 'succeeded')
    assert.equal(typeof database.prepare('SELECT sha256 FROM digests WHERE path=?').get(file.path)!.sha256, 'string')
  }
  assert.equal(store.prune(0), files.length)
  assert.equal(database.prepare('SELECT COUNT(*) count FROM digests').get()!.count, files.length)
  console.log(JSON.stringify({ root, files: files.length, databasePath, finalVersion: store.version,
    checked: ['atomic submit', 'optimistic version conflict', 'payload update', 'promotion', 'persisted claim',
      'connection reopen', 'explicit recovery', 'stale claim rejection', 'two-connection claim exclusion',
      'killed process recovery after confirmed exit', 'caller transaction preserved', 'SQLite constraint rollback',
      'scheduled real-file digest and transactional settlement', 'history pruning retains business results'] }, null, 2))
} finally { await scheduler.close(); database.close() }
