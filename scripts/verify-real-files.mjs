import assert from 'node:assert/strict'
import { createReadStream } from 'node:fs'
import { readdir, stat } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { join, resolve } from 'node:path'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { TaskScheduler, TaskCapacityError, QueueVersionConflict } from '../dist/index.js'

// 只读取调用方提供的真实文件；不生成测试载荷，不修改来源目录。
const root = resolve(process.argv[2] ?? '')
if (!process.argv[2]) throw new Error('Pass an existing directory containing real files')
const files = []
for (const entry of await readdir(root, { withFileTypes: true })) {
  if (!entry.isFile() || !/\.(exe|dll|pak|dat|bin|html)$/.test(entry.name)) continue
  const path = join(root, entry.name), info = await stat(path)
  files.push({ path, bytes: info.size })
}
files.sort((a, b) => b.bytes - a.bytes)
assert(files.length >= 6, 'Need at least six existing files')
const trace = [], baseline = new Map()
const lag = monitorEventLoopDelay({ resolution: 10 }); lag.enable()
const began = performance.now()
const scheduler = new TaskScheduler({ resources: { disk: 2 }, concurrency: 2, capacity: files.length * 4,
  quantumMs: 1, rings: [
    { name: 'foreground', rank: 0, preempts: ['background'] },
    { name: 'background', rank: 1, maxBypass: 3 },
  ] })
const events = []
const gaps = []
scheduler.subscribe({ event: event => events.push(event), gap: gap => gaps.push(gap) })
const liveLanes = new Set()
const definition = {
  name: 'files.sha256', mode: 'restartable', key: input => input.path,
  lane: input => input.lane, segment: input => input.segment, resources: () => ({ disk: 1 }),
  execute: async (input, execution) => {
    if (input.lane) { assert(!liveLanes.has(input.lane)); liveLanes.add(input.lane) }
    trace.push({ phase: 'enter', path: input.path, ring: execution.demand.ring, segment: input.segment, time: performance.now() })
    const hash = createHash('sha256')
    let bytes = 0
    try {
      for await (const chunk of createReadStream(input.path, { signal: execution.signal, highWaterMark: 65536 })) {
        hash.update(chunk); bytes += chunk.length
      }
      execution.assertCurrent()
      assert.equal(bytes, (await stat(input.path)).size)
      return hash.digest('hex')
    } finally {
      if (input.lane) liveLanes.delete(input.lane)
      trace.push({ phase: 'exit', path: input.path, aborted: execution.signal.aborted, time: performance.now() })
    }
  },
}
const queue = scheduler.register(definition)
try {
  scheduler.pause()
  const background = queue.submit(files[0], { ring: 'background', priority: 100 })
  const frontLow = queue.submit(files[1], { ring: 'foreground', priority: -20 })
  const frontHigh = queue.submit(files[2], { ring: 'foreground', priority: 20 })
  scheduler.resume()
  const initial = await Promise.all([background.result, frontLow.result, frontHigh.result])
  for (const [index, file] of files.slice(0, 3).entries()) baseline.set(file.path, initial[index])
  const starts = events.filter(event => event.action === 'started')
  assert.equal(starts[0].task.id, frontHigh.id)
  assert.equal(starts[1].task.id, frontLow.id)
  assert.equal(scheduler.snapshot().tasks.length, 0)

  // 已开始的后台真实文件读取必须实际退出，前台才能领取其资源。
  const preemptStart = events.length
  const low = files.slice(0, 2).map(file => queue.submit(file, { ring: 'background', priority: 0 }))
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { off(); reject(new Error('No background execution started')) }, 5000)
    const off = scheduler.subscribe({ event: event => {
      if (event.action === 'started' && event.task.id === low[1].id) {
        off(); clearTimeout(timer)
        setImmediate(resolve)
      }
    }, gap: gap => { off(); clearTimeout(timer); reject(new Error(`Unexpected event gap: ${JSON.stringify(gap)}`)) } })
  })
  const foreground = queue.submit(files[3], { ring: 'foreground', priority: 0 })
  await Promise.all([...low.map(lease => lease.result), foreground.result])
  const preemptEvents = events.slice(preemptStart)
  assert(preemptEvents.some(event => event.action === 'interrupt' && event.task.stop === 'preempt'), 'Real workload did not exercise preemption')

  // 同 key 共享需求与跨环撤销；开始前操作，序列可从真实调度事件核对。
  scheduler.pause()
  const sharedLow = queue.submit(files[4], { ring: 'background', priority: 0 })
  const sharedHigh = queue.submit(files[4], { ring: 'foreground', priority: 10 })
  assert.equal(sharedLow.id, sharedHigh.id)
  assert.equal(scheduler.snapshot().tasks[0].ring, 'foreground')
  const sharedCancellation = assert.rejects(sharedHigh.result, error => error.kind === 'cancel')
  sharedHigh.release()
  assert.equal(scheduler.snapshot().tasks[0].ring, 'background')
  scheduler.resume(); await Promise.all([sharedLow.result, sharedCancellation])

  // 同 lane 的不同文件严格串行；后一阶段必须等前一阶段全部退出。
  scheduler.pause()
  const segmentStart = events.length
  const staged = queue.submitBatch(files.slice(0, 6).map((file, index) => ({
    input: { ...file, lane: index < 2 ? 'file-verification' : undefined, segment: index < 3 ? 0 : 1 },
    options: { ring: 'background', priority: index },
  })), queue.version)
  scheduler.resume(); await Promise.all(staged.map(lease => lease.result))
  const stageEvents = events.slice(segmentStart)
  const lastStageZero = Math.max(...stageEvents.filter(event => event.action === 'succeeded' && event.task.segment === 0).map(event => event.sequence))
  const firstStageOne = Math.min(...stageEvents.filter(event => event.action === 'started' && event.task.segment === 1).map(event => event.sequence))
  assert(firstStageOne > lastStageZero)

  // 乐观并发取消、关闭重挂：旧执行仍持有实际资源直到流退出。
  const old = queue.submit(files[0], { ring: 'background', priority: 0 })
  const oldCancellation = assert.rejects(old.result, error => error.kind === 'unregister')
  await new Promise(resolve => setImmediate(resolve))
  const closing = queue.close()
  const replacement = scheduler.register(definition)
  const renewed = replacement.submit(files[0], { ring: 'foreground', priority: 0 })
  assert.equal(await renewed.result, baseline.get(files[0].path))
  await closing; await replacement.onIdle({ timeoutMs: 5000 })
  await replacement.close()
  await oldCancellation

  const small = new TaskScheduler({ resources: { disk: 1 }, concurrency: 1, capacity: 2, capacityBehavior: 'throw' })
  const limited = small.register(definition)
  small.pause()
  assert.throws(() => limited.submitBatch(files.slice(0, 3).map(input => ({ input, options: { priority: 0 } }))), TaskCapacityError)
  assert.equal(small.snapshot().tasks.length, 0)
  const admitted = limited.submitBatch(files.slice(0, 2).map(input => ({ input, options: { priority: 0 } })))
  assert.throws(() => limited.cancelPending(admitted[0].id, limited.version - 1), QueueVersionConflict)
  const pendingCancellation = assert.rejects(admitted[0].result, error => error.kind === 'cancel')
  limited.cancelPending(admitted[0].id, limited.version)
  small.resume(); await Promise.all([admitted[1].result, pendingCancellation])
  await small.drain({ timeoutMs: 5000 })
  assert.throws(() => limited.submit(files[4], { priority: 0 }))
  await small.close()

  await scheduler.onIdle({ timeoutMs: 5000 })
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.tasks.length, 0)
  assert.equal(gaps.length, 0)
  assert(Object.values(snapshot.indexes).every(value => value === 0))
  assert(Object.values(snapshot.used).every(value => value === 0))
  assert(snapshot.rings.every(ring => ring.tasks === 0 && ring.queued === 0 && ring.running === 0))
  console.log(JSON.stringify({ root, files: files.length, inputBytes: files.reduce((sum, file) => sum + file.bytes, 0),
    elapsedMs: performance.now() - began, eventLoopP99Ms: lag.percentile(99) / 1e6,
    starts: events.filter(event => event.action === 'started').length,
    preemptions: events.filter(event => event.action === 'interrupt' && event.task.stop === 'preempt').length,
    executionExits: trace.filter(entry => entry.phase === 'exit').length,
    checked: ['ring order', 'ring priority', 'real stream preemption', 'shared demand downgrade', 'lane exclusion', 'segment barrier', 'atomic admission', 'queue version', 'close/re-register', 'drain'],
  }, null, 2))
} finally {
  lag.disable()
  await scheduler.close()
}
