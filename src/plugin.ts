import type { Context } from '@deepseek-ai/cordis'
import { TaskScheduler } from './scheduler.js'
import type { SchedulerOptions, TaskEvent, TaskEventGap } from './types.js'

export const name = 'task-scheduler'
export type Config = SchedulerOptions

declare module '@deepseek-ai/cordis' {
  interface Context { taskScheduler: TaskScheduler }
  interface Events {
    'tasks:changed'(event: TaskEvent): void
    'tasks:gap'(gap: TaskEventGap): void
  }
}

/** Cordis 生命周期持有任务与执行资源，注销等待清理完成。 */
export function apply(ctx: Context, config: Config): void {
  const scheduler = new TaskScheduler(config)
  ctx.provide('taskScheduler', scheduler)
  ctx.effect(() => scheduler.subscribe({
    event: event => ctx.emit('tasks:changed', event),
    gap: gap => ctx.emit('tasks:gap', gap),
  }))
  ctx.effect(() => () => scheduler.close())
}
