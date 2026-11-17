import assert from 'node:assert/strict'
import { createReadStream } from 'node:fs'
import { readdir, readFile, stat } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { join, resolve } from 'node:path'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { TaskScheduler, TaskCapacityError } from '../dist/index.js'
import type { Execution, TaskDefinition, TaskEvent, TaskQueue } from '../dist/index.js'

// 工作负载始终来自现有发行文件。readFile 的完整摘要独立于被调度的流式读取，
// 同时核验每次抢占重启后的完整结果；不创建替代素材、不用 sleep 占据执行槽。
interface FileInput { path: string; bytes: number; lane?: string; segment?: number }
const directory = process.argv[2]
if (!directory) throw new Error('Pass an existing directory containing real files')
const root = resolve(directory)
const files: FileInput[] = []
for (const entry of await readdir(root, { withFileTypes: true })) {
  if (!entry.isFile() || !/\.(exe|dll|pak|dat|bin|html)$/.test(entry.name)) continue
  const path = join(root, entry.name)
  files.push({ path, bytes: (await stat(path)).size })
}
files.sort((a, b) => b.bytes - a.bytes)
assert(files.length >= 6, 'Need at least six existing files')
const baseline = new Map<string, string>()
for (const file of files) baseline.set(file.path, createHash('sha256').update(await readFile(file.path)).digest('hex'))
const lag = monitorEventLoopDelay({ resolution: 10 })
lag.enable()
const began = performance.now()
let readBytes = 0, exits = 0
const streamHash = async (input: FileInput, execution: Execution<never>): Promise<string> => {
  const hash = createHash('sha256')
  let bytes = 0
  try {
    for await (const chunk of createReadStream(input.path, { signal: execution.signal, highWaterMark: 65536 })) {
      hash.update(chunk); bytes += chunk.length; readBytes += chunk.length
    }
    execution.assertCurrent()
    assert.equal(bytes, input.bytes)
    const digest = hash.digest('hex')
    assert.equal(digest, baseline.get(input.path), `Full-file digest mismatch: ${input.path}`)
    return digest
  } finally { exits++ }
}
const definition: TaskDefinition<FileInput, string> = {
  name: 'files.digest', mode: 'restartable', key: input => input.path,
  resources: () => ({ disk: 1, hash: 1 }), lane: input => input.lane,
  segment: input => input.segment, execute: streamHash,
}
const scheduler = new TaskScheduler({ resources: { disk: 2, hash: 1 }, concurrency: 2,
  capacity: 64, quantumMs: 2, historyLimit: 64,
  rings: [{ name: 'foreground', rank: 0, preempts: ['background'] },
    { name: 'background', rank: 1, maxBypass: 3 }],
})
const events: TaskEvent[] = []
const unexpectedGaps: { fromSequence: number; toSequence: number }[] = []
scheduler.subscribe({ event: event => events.push(event), gap: gap => unexpectedGaps.push(gap) })
const queue = scheduler.register(definition)
const reader = scheduler.register<FileInput, number>({
  name: 'files.read', mode: 'restartable', key: input => input.path, resources: () => ({ disk: 1 }),
  execute: async (input, execution) => {
    let bytes = 0
    for await (const chunk of createReadStream(input.path, { signal: execution.signal, highWaterMark: 65536 })) {
      bytes += chunk.length; readBytes += chunk.length
    }
    execution.assertCurrent(); assert.equal(bytes, input.bytes)
    return bytes
  },
})
const deadline = setTimeout(() => { console.error('Sustained workload timed out'); void scheduler.close() }, 30_000)
try {
  scheduler.pause()
  assert.throws(() => queue.submitBatch([
    { input: { ...files[0]!, lane: 'one' }, options: { priority: 0 } },
    { input: { ...files[0]!, lane: 'two' }, options: { priority: 0 } },
  ]), /Conflicting scheduling constraints/)
  assert.equal(scheduler.snapshot().tasks.length, 0)
  scheduler.resume()

  let backgroundDone = false, foregroundCount = 0, reads = 0, countAtBackgroundCompletion = -1
  const background = queue.submit(files[0]!, { ring: 'background', priority: 0 })
  const observedBackground = background.result.then(() => {
    backgroundDone = true; countAtBackgroundCompletion = foregroundCount
  })
  await new Promise<void>(resolve => setImmediate(resolve))
  // 两个生产者始终有真实 I/O：一个连续争用 hash，另一个争用 disk。
  // 必须在生产者停止前等到后台完成，不能靠停止前台后 drain 来证明公平。
  const producer = async (): Promise<void> => {
    while (!backgroundDone || foregroundCount < 32) {
      const file = files[1 + foregroundCount % (files.length - 1)]!
      await queue.submit(file, { ring: 'foreground', priority: 10 }).result
      foregroundCount++
    }
  }
  const readProducer = async (): Promise<void> => {
    while (!backgroundDone || reads < 32) {
      await reader.submit(files[1 + reads % (files.length - 1)]!, { ring: 'foreground', priority: 0 }).result
      reads++
    }
  }
  await Promise.all([observedBackground, producer(), readProducer()])
  assert(countAtBackgroundCompletion >= 3, 'Workload did not exercise sustained foreground contention')
  assert(events.some(event => event.action === 'interrupt' && event.task.id === background.id), 'Background was not interrupted')
  const backgroundAttempts = events.filter(event => event.action === 'started' && event.task.id === background.id).length

  // 阶段号在 started 事件交付前就已经封闭；不能趁 execute 微任务尚未进入回插旧阶段。
  scheduler.pause()
  const staged = queue.submit({ ...files[2]!, segment: 1 }, { priority: 0 })
  let stageChecked = false
  const off = scheduler.subscribe({ event: event => {
    if (event.action !== 'started' || event.task.id !== staged.id) return
    assert.throws(() => queue.submit({ ...files[3]!, segment: 0 }, { priority: 0 }), /Segment/)
    stageChecked = true
  }, gap: gap => { throw new Error(`Unexpected stage event gap: ${JSON.stringify(gap)}`) } })
  scheduler.resume(); await staged.result; off(); assert(stageChecked)

  // 仍以真实文件为任务输入，验证不可能完成的编排会明确拒绝并清理依赖。
  const guarded = new TaskScheduler({ resources: { disk: 1, hash: 1 } })
  try {
    const childQueue = guarded.register(definition)
    const laneParent = guarded.register<FileInput, string>({ name: 'files.lane-parent', mode: 'orchestrator',
      key: input => input.path, lane: input => input.lane, resources: () => ({}),
      execute: async (input, execution) => execution.submitChild(childQueue, input).result,
    })
    await assert.rejects(laneParent.submit({ ...files[4]!, lane: 'shared-file-operation' }, { priority: 0 }).result, /ancestor lane/)
    let cyclic: TaskQueue<FileInput, string>
    cyclic = guarded.register<FileInput, string>({ name: 'files.cyclic', mode: 'orchestrator',
      key: input => input.path, resources: () => ({}),
      execute: async (input, execution) => execution.submitChild(cyclic, input).result,
    })
    await assert.rejects(cyclic.submit(files[4]!, { priority: 0 }).result, /cycle/)
    let segmented: TaskQueue<FileInput, string>
    segmented = guarded.register<FileInput, string>({ name: 'files.segment-parent', mode: 'orchestrator',
      key: input => JSON.stringify([input.path, input.segment]), segment: input => input.segment, resources: () => ({}),
      execute: async (input, execution) => execution.submitChild(segmented, { ...input, segment: 1 }).result,
    })
    await assert.rejects(segmented.submit({ ...files[4]!, segment: 0 }, { priority: 0 }).result, /ancestor segment/)
    await guarded.onIdle({ timeoutMs: 5000 })
    assert(Object.values(guarded.snapshot().indexes).every(value => value === 0))
  } finally { await guarded.close() }

  // 编排只持有依赖；同一实际文件的摘要仍由资源执行器执行。
  const workflow = scheduler.register<FileInput, string>({ name: 'files.workflow', mode: 'orchestrator',
    key: input => input.path, resources: () => ({}),
    execute: async (input, execution) => execution.submitChild(queue, input).result,
  })
  const parent = workflow.submit(files[4]!, { ring: 'background', priority: 0 })
  const draining = scheduler.drain({ timeoutMs: 5000 })
  const result = await parent.result
  assert.equal(result, baseline.get(files[4]!.path))
  await draining

  await scheduler.onIdle({ timeoutMs: 5000 })
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.tasks.length, 0)
  assert.equal(unexpectedGaps.length, 0)
  assert(Object.values(snapshot.used).every(value => value === 0))
  assert(Object.values(snapshot.indexes).every(value => value === 0), JSON.stringify(snapshot.indexes))

  const limited = new TaskScheduler({ resources: { disk: 1, hash: 1 }, capacity: 1, capacityBehavior: 'reject' })
  try {
    limited.pause()
    const limitedQueue = limited.register(definition)
    const rejected = limitedQueue.submitBatch(files.slice(0, 2).map(input => ({ input, options: { priority: 0 } })))
    await Promise.all(rejected.map(lease => assert.rejects(lease.result, TaskCapacityError)))
    assert.equal(limited.snapshot().tasks.length, 0)
  } finally { await limited.close() }
  // 人为缩小观测保留量，提交的仍是现有文件，验证溢出显式报 gap 且读取照常完成。
  const bounded = new TaskScheduler({ resources: { disk: 1, hash: 1 }, historyLimit: 4, capacity: files.length })
  try {
    const gaps: { fromSequence: number; toSequence: number }[] = []
    bounded.subscribe({ event: () => {}, gap: gap => gaps.push(gap) })
    const boundedQueue = bounded.register(definition)
    const batch = boundedQueue.submitBatch(files.map(input => ({ input, options: { priority: 0 } })))
    await Promise.all(batch.map(lease => lease.result))
    assert(gaps.length > 0)
    assert.equal(gaps[0]!.fromSequence, 1)
    assert(bounded.snapshot().history.length <= 4)
  } finally { await bounded.close() }
  console.log(JSON.stringify({ root, files: files.length, inputBytes: files.reduce((sum, file) => sum + file.bytes, 0),
    elapsedMs: performance.now() - began, readBytes, foregroundCount, reads, countAtBackgroundCompletion,
    backgroundAttempts, exits, eventLoopP99Ms: lag.percentile(99) / 1e6, finalIndexes: snapshot.indexes,
    checked: ['independent full-file digests', 'sustained foreground contention', 'mixed resource groups',
      'background completes before producers stop', 'index reclamation', 'conflicting shared batch',
      'segment admission window', 'child cycle/lane/segment rejection', 'orchestrated child during drain',
      'whole-batch rejection', 'bounded observer history with explicit gap'],
  }, null, 2))
} finally { clearTimeout(deadline); lag.disable(); await scheduler.close() }
