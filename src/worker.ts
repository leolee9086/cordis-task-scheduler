import type { Worker } from 'node:worker_threads'
import { TaskInterrupted } from './types.js'

interface Slot { worker: Worker; busy: boolean; stopping?: Promise<number>; cancel?: (error: Error) => void }

/** 每个 Worker 同时只执行一个请求，不在执行器内部再藏一条 FIFO 队列。 */
export class WorkerExecutor<I, O> {
  private readonly slots = new Set<Slot>()
  private closed = false
  private closing?: Promise<void>
  constructor(private readonly capacity: number, private readonly create: () => Worker,
    private readonly decode: (message: unknown) => O) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error('Worker capacity must be positive')
  }

  execute(input: I, signal: AbortSignal): Promise<O> {
    if (this.closed) return Promise.reject(new Error('Worker executor is closed'))
    if (signal.aborted) return Promise.reject(signal.reason)
    let slot = [...this.slots].find(candidate => !candidate.busy && !candidate.stopping)
    if (!slot) {
      if (this.slots.size >= this.capacity) return Promise.reject(new Error('Worker capacity exceeded; schedule before dispatch'))
      const worker = this.create()
      slot = { worker, busy: false }
      this.slots.add(slot)
      const current = slot
      // 常驻监听器覆盖空闲期崩溃；不留下 EventEmitter 未处理 error。
      worker.on('error', error => {
        if (current.cancel) current.cancel(error)
        else void this.stop(current).catch(error => console.error('[task-worker] idle termination failed', error))
      })
      worker.on('exit', code => {
        this.slots.delete(current)
        current.cancel?.(new Error(`Worker exited (${code})`))
      })
    }
    const current = slot
    current.busy = true
    return new Promise<O>((resolve, reject) => {
      let settled = false
      const cleanup = (): void => {
        signal.removeEventListener('abort', onAbort)
        current.worker.off('message', onMessage)
        current.cancel = undefined
      }
      const cancel = (error: Error): void => {
        if (settled) return
        settled = true
        cleanup()
        // terminate 的完成回执是执行资源释放边界，拒绝 Promise 不能早于它。
        void this.stop(current).then(() => reject(error), cause => reject(new AggregateError([error, cause], 'Worker termination failed')))
      }
      const onAbort = (): void => cancel(signal.reason instanceof Error ? signal.reason : new TaskInterrupted('cancel'))
      const onMessage = (message: unknown): void => {
        if (settled) return
        if (signal.aborted) { onAbort(); return }
        let value: O
        try { value = this.decode(message) } catch (error) {
          cancel(error instanceof Error ? error : new Error(String(error)))
          return
        }
        settled = true
        cleanup()
        current.busy = false
        resolve(value)
      }
      current.cancel = cancel
      signal.addEventListener('abort', onAbort, { once: true })
      current.worker.on('message', onMessage)
      try { current.worker.postMessage(input) } catch (error) {
        cancel(error instanceof Error ? error : new Error(String(error)))
      }
    })
  }

  private stop(slot: Slot): Promise<number> {
    slot.stopping ??= slot.worker.terminate().then(code => { this.slots.delete(slot); return code })
    return slot.stopping
  }

  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    const slots = [...this.slots]
    for (const slot of slots) slot.cancel?.(new TaskInterrupted('close'))
    this.closing = Promise.all(slots.map(slot => this.stop(slot))).then(() => undefined)
    return this.closing
  }
}
