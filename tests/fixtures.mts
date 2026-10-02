import type { Server } from 'node:http'
import { setTimeout as sleep } from 'node:timers/promises'

export async function until<T>(read: () => T | Promise<T>, check: (value: T) => boolean, timeout = 10000): Promise<T> {
  const end = Date.now() + timeout
  for (;;) { const value = await read(); if (check(value)) return value; if (Date.now() > end) throw new Error('等待条件超时'); await sleep(20) }
}
export async function listen(server: Server): Promise<string> {
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  return `http://127.0.0.1:${(server.address() as any).port}`
}
export async function closeServer(server: Server): Promise<void> { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())) }

