import { mkdir, readFile, open, rename, unlink } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomUUID } from 'node:crypto'

export const VERSION = 'b1-Rc5'
export const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
export const dataRoot = path.resolve(process.env.ME_DATA_DIR ?? path.join(root, 'data'))

export function object(value: any): Record<string, any> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('参数需要是对象')
  return value
}
export function text(value: any, name: string, max = 32768): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`${name}为空或过长`)
  return value
}
export function integer(value: any, name: string, min: number, max: number): number {
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name}超出范围`)
  return value
}
export function log(level: string, module: string, message: string): void {
  console.error(`[${level.toUpperCase()}] ${message.replace(/[\r\n]+/g, ' ')}`)
}
export async function jsonFile(file: string, defaults: any): Promise<any> {
  try { return JSON.parse(await readFile(file, 'utf8')) } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    return structuredClone(defaults)
  }
}
export async function writeJSON(file: string, value: any): Promise<void> {
  await mkdir(path.dirname(file), { recursive: true, mode: 0o700 })
  const temporary = `${file}.${randomUUID()}.tmp`
  const handle = await open(temporary, 'wx', 0o600)
  try {
    await handle.writeFile(JSON.stringify(value, null, 2) + '\n'); await handle.sync(); await handle.close()
    await rename(temporary, file)
  } catch (error) { await handle.close().catch(() => {}); await unlink(temporary).catch(() => {}); throw error }
}

// 回收等待实际执行结束，取消只关闭继续工作的入口
export class Work {
  controller = new AbortController()
  active = new Set<Promise<any>>()
  constructor(signal?: AbortSignal) {
    if (signal?.aborted) this.controller.abort()
    signal?.addEventListener('abort', () => this.controller.abort(), { once: true })
  }
  get signal(): AbortSignal { return this.controller.signal }
  run<T>(job: (signal: AbortSignal) => Promise<T>): Promise<T> {
    this.signal.throwIfAborted()
    const pending = Promise.resolve().then(() => job(this.signal))
    this.active.add(pending)
    pending.then(() => this.active.delete(pending), () => this.active.delete(pending))
    return pending
  }
  async close(): Promise<void> { this.controller.abort(); await Promise.allSettled([...this.active]) }
}

