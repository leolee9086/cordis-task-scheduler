import type { ChildProcess, Serializable } from 'node:child_process'
import { TaskInterrupted } from './types.js'

interface Slot {
  child: ChildProcess
  busy: boolean
  closed: Promise<void>
  stopping: boolean
  cancel?: (error: Error) => void
}

/** 原生负载的进程隔离执行端；每个进程只接一个请求，不另设内部 FIFO。
 * 工厂必须创建带 IPC 的独立工作进程；它不得再启动无人管理的子进程。
 * 每个输入只返回一个最终消息。取消等待 close，包含进程退出及 stdio 关闭。
 */
export class ChildProcessExecutor<I extends Serializable, O> {
  private readonly slots = new Set<Slot>()
  private closed = false
  private closing?: Promise<void>
  constructor(private readonly capacity: number, private readonly create: () => ChildProcess,
    private readonly decode: (message: unknown) => O) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) throw new Error('Process capacity must be positive')
  }

  execute(input: I, signal: AbortSignal): Promise<O> {
    if (this.closed) return Promise.reject(new Error('Process executor is closed'))
    if (signal.aborted) return Promise.reject(signal.reason)
    let slot = [...this.slots].find(candidate => !candidate.busy && !candidate.stopping)
    if (!slot) {
      if (this.slots.size >= this.capacity) return Promise.reject(new Error('Process capacity exceeded; schedule before dispatch'))
      const child = this.create()
      let onClosed!: () => void
      const current: Slot = { child, busy: false, stopping: false, closed: new Promise<void>(resolve => { onClosed = resolve }) }
      this.slots.add(current)
      child.once('close', (code, exitSignal) => {
        current.stopping = true
        this.slots.delete(current)
        onClosed()
        current.cancel?.(new Error(`Process exited (${code ?? exitSignal})`))
      })
      child.on('error', error => {
        if (current.cancel) current.cancel(error)
        else void this.stop(current)
      })
      child.on('disconnect', () => {
        if (current.cancel) current.cancel(new Error('Process IPC disconnected'))
        else void this.stop(current)
      })
      slot = current
    }
    const current = slot
    current.busy = true
    return new Promise<O>((resolve, reject) => {
      let settled = false
      const cleanup = (): void => {
        signal.removeEventListener('abort', onAbort)
        current.child.off('message', onMessage)
        current.cancel = undefined
      }
      const cancel = (error: Error): void => {
        if (settled) return
        settled = true; cleanup()
        // SIGTERM/TerminateProcess 的发送成功不是退出证据，继续持有额度直到 close。
        void this.stop(current).then(() => reject(error))
      }
      const onAbort = (): void => cancel(signal.reason instanceof Error ? signal.reason : new TaskInterrupted('cancel'))
      const onMessage = (message: unknown): void => {
        if (settled) return
        if (signal.aborted) { onAbort(); return }
        let value: O
        try { value = this.decode(message) } catch (error) {
          cancel(error instanceof Error ? error : new Error(String(error))); return
        }
        settled = true; cleanup(); current.busy = false; resolve(value)
      }
      current.cancel = cancel
      signal.addEventListener('abort', onAbort, { once: true })
      current.child.on('message', onMessage)
      try {
        if (!current.child.connected || !current.child.send) throw new Error('Process factory must provide connected IPC')
        current.child.send(input, error => { if (error) cancel(error) })
      } catch (error) { cancel(error instanceof Error ? error : new Error(String(error))) }
      if (signal.aborted) onAbort()
    })
  }

  private stop(slot: Slot): Promise<void> {
    if (!slot.stopping) {
      slot.stopping = true
      if (slot.child.exitCode === null && slot.child.signalCode === null) {
        // SIGKILL 在 Windows 对应终止进程；使用独立进程隔离才能停止原生线程。
        try { slot.child.kill('SIGKILL') } catch (error) { console.error('[task-process] termination failed; awaiting close', error) }
      }
    }
    return slot.closed
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
