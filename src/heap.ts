/** 可定位二叉堆；取消与提权直接修复索引，不遗留辅助堆幽灵项。 */
export class IndexedHeap<T extends { id: number }> {
  private readonly items: T[] = []
  private readonly positions = new Map<number, number>()
  constructor(private readonly compare: (a: T, b: T) => number) {}
  get size(): number { return this.items.length }
  peek(): T | undefined { return this.items[0] }
  values(): readonly T[] { return this.items }
  /** 老化批次改变比较权重时线性重建，避免逐项 O(n log n) 修复。 */
  rebuild(): void {
    for (let index = (this.items.length >>> 1) - 1; index >= 0; index--) this.down(index)
  }
  upsert(item: T): void {
    const previous = this.positions.get(item.id)
    if (previous === undefined) {
      this.positions.set(item.id, this.items.length)
      this.items.push(item)
      this.up(this.items.length - 1)
    } else {
      this.items[previous] = item
      this.down(this.up(previous))
    }
  }
  remove(id: number): T | undefined {
    const index = this.positions.get(id)
    if (index === undefined) return undefined
    const removed = this.items[index]!
    const last = this.items.pop()!
    this.positions.delete(id)
    if (index < this.items.length) {
      this.items[index] = last
      this.positions.set(last.id, index)
      this.down(this.up(index))
    }
    return removed
  }
  private swap(a: number, b: number): void {
    const first = this.items[a]!, second = this.items[b]!
    this.items[a] = second; this.items[b] = first
    this.positions.set(first.id, b); this.positions.set(second.id, a)
  }
  private up(index: number): number {
    while (index > 0) {
      const parent = (index - 1) >>> 1
      if (this.compare(this.items[index]!, this.items[parent]!) >= 0) break
      this.swap(index, parent); index = parent
    }
    return index
  }
  private down(index: number): void {
    for (;;) {
      const left = index * 2 + 1, right = left + 1
      let best = index
      if (left < this.items.length && this.compare(this.items[left]!, this.items[best]!) < 0) best = left
      if (right < this.items.length && this.compare(this.items[right]!, this.items[best]!) < 0) best = right
      if (best === index) return
      this.swap(index, best); index = best
    }
  }
}
