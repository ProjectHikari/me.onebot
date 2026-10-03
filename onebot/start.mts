import { spawn, type ChildProcess } from 'node:child_process'
import { access, readFile } from 'node:fs/promises'
import { constants } from 'node:fs'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { WebSocket } from 'ws'
import { integer, log, object, text } from '../shared/runtime.mts'

export type StartupConfig = { enabled: boolean; directory: string; timeout: number; autoHook: boolean }
export function startupConfig(raw: any = {}): StartupConfig {
  const input = object(raw)
  if (input.enabled !== undefined && typeof input.enabled !== 'boolean') throw new Error('SnowLuma自动启动选项无效')
  if (input.autoHook !== undefined && typeof input.autoHook !== 'boolean') throw new Error('SnowLuma自动Hook选项无效')
  const directory = input.directory || process.env.ME_SNOWLUMA_DIR || ''
  if (directory && !path.isAbsolute(text(directory, 'SnowLuma目录', 4096))) throw new Error('SnowLuma目录需要使用绝对路径')
  if (input.enabled && !directory) throw new Error('请设置SnowLuma目录')
  return { autoHook: input.autoHook ?? false, enabled: input.enabled ?? !!process.env.ME_SNOWLUMA_DIR, directory, timeout: integer(input.timeout ?? 30000, 'SnowLuma启动超时', 1000, 120000) }
}
async function probe(url: string, token: string, signal: AbortSignal): Promise<boolean> {
  signal.throwIfAborted()
  const socket = new WebSocket(url, { headers: token ? { Authorization: `Bearer ${token}` } : {}, handshakeTimeout: 1000, maxPayload: 524288 })
  return new Promise(resolve => {
    let ready = false
    const stop = () => socket.terminate(), timer = setTimeout(stop, 1500)
    signal.addEventListener('abort', stop, { once: true })
    socket.on('error', () => {})
    socket.on('open', () => socket.send(JSON.stringify({ action: 'get_login_info', params: {}, echo: 'me.onebot.startup' }), error => { if (error) stop() }))
    socket.on('message', bytes => {
      try { const value = JSON.parse(bytes.toString()); if (value.echo === 'me.onebot.startup') { ready = value.status === 'ok' && value.retcode === 0; socket.close() } } catch { stop() }
    })
    socket.once('close', () => { clearTimeout(timer); signal.removeEventListener('abort', stop); resolve(ready) })
    if (signal.aborted) stop()
  })
}
export class SnowLumaStartup {
  config: StartupConfig
  child?: ChildProcess
  exited?: Promise<void>
  state = 'disabled'
  stopping = false
  error = ''
  constructor(config: StartupConfig) { this.config = config }
  async open(connection: { mode: string; url: string; token: string }, signal: AbortSignal): Promise<void> {
    if (!this.config.enabled) return
    signal.throwIfAborted()
    if (connection.mode === 'forward' && await probe(connection.url, connection.token, signal)) {
      this.state = 'external'; log('OK', 'me.onebot', '复用已运行的SnowLuma'); return
    }
    const directory = this.config.directory
    await access(path.join(directory, 'index.mjs'))
    // 外部进程只复用，不冒领或终止它的PID
    try {
      const pid = Number((await readFile(path.join(directory, 'runtime.pid'), 'utf8')).trim())
      if (Number.isSafeInteger(pid) && pid > 0) {
        try { process.kill(pid, 0); this.state = 'external'; log('WARN', 'me.onebot', 'SnowLuma已运行，等待OneBot连接'); return }
        catch (error) { if ((error as NodeJS.ErrnoException).code === 'EPERM') throw new Error('SnowLuma进程无法验证'); if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error }
      }
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
    let command = path.join(directory, 'node')
    try { await access(command, constants.X_OK) } catch { command = process.execPath }
    this.state = 'starting'; log('INFO', 'me.onebot', '正在启动SnowLuma')
    const child = this.child = spawn(command, ['index.mjs'], { cwd: directory, env: { ...process.env, SNOWLUMA_HOOK_AUTOLOAD: this.config.autoHook ? '1' : '0' }, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] })
    this.exited = new Promise(resolve => child.once('close', () => { if (!this.stopping) { this.state = 'failed'; this.error = 'SnowLuma进程已退出'; log('ERROR', 'me.onebot', this.error) }; resolve() }))
    child.on('error', () => { this.error = 'SnowLuma启动失败' })
    for (const stream of [child.stdout!, child.stderr!]) {
      let partial = ''
      const output = (line: string) => {
        line = line.replace(/\x1b\[[0-9;]*m/g, '')
        if (connection.token) line = line.split(connection.token).join('[已隐藏]')
        line = line.replace(/((?:access[_ -]?token|authorization|password|密码|令牌)\s*[:=]\s*)\S+/gi, '$1[已隐藏]')
        const parsed = /^(?:\d{2}:\d{2}:\d{2}\s+)?(?:\[)?(INFO|OK|WARN|ERROR|FAILED|DEBUG)(?:\])?\s+(.*)$/.exec(line)
        log(parsed?.[1] === 'DEBUG' ? 'INFO' : parsed?.[1] ?? 'INFO', 'me.onebot', `SnowLuma - ${parsed?.[2] ?? line}`)
      }
      stream.setEncoding('utf8'); stream.on('data', value => {
        partial += value
        let end: number
        while ((end = partial.indexOf('\n')) >= 0) { output(partial.slice(0, end)); partial = partial.slice(end + 1) }
        while (Buffer.byteLength(partial) > 4096) { output(partial.slice(0, 1000)); partial = partial.slice(1000) }
      }); stream.on('end', () => { if (partial) output(partial) })
    }
    await new Promise<void>((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject) })
    this.state = 'running'; log('OK', 'me.onebot', 'SnowLuma进程已启动')
    if (connection.mode !== 'forward') return
    const end = Date.now() + this.config.timeout
    while (Date.now() < end) {
      signal.throwIfAborted()
      if (this.state === 'failed') throw new Error(this.error)
      if (await probe(connection.url, connection.token, signal)) { log('OK', 'me.onebot', 'SnowLuma接口已就绪'); return }
      await sleep(500, undefined, { signal })
    }
    log('WARN', 'me.onebot', 'SnowLuma已启动，等待QQ登录或Hook完成')
  }
  status(): any { return { state: this.state, managed: !!this.child, pid: this.child?.pid, error: this.error } }
  async close(): Promise<void> {
    const child = this.child
    if (!child?.pid) return
    this.stopping = true
    const kill = (signal: NodeJS.Signals) => {
      try { if (process.platform === 'win32') child.kill(signal); else process.kill(-child.pid!, signal) } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error }
    }
    kill('SIGTERM')
    const timer = setTimeout(() => { try { kill('SIGKILL') } catch { this.error = 'SnowLuma进程无法终止' } }, 3000)
    try { await this.exited } finally { clearTimeout(timer) }
    if (process.platform !== 'win32') {
      const alive = () => { try { process.kill(-child.pid!, 0); return true } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false; throw error } }
      let end = Date.now() + 3000
      while (alive() && Date.now() < end) await sleep(25)
      if (alive()) { kill('SIGKILL'); end = Date.now() + 1000; while (alive() && Date.now() < end) await sleep(25) }
      if (alive()) throw new Error('SnowLuma进程组未完整释放')
    }
    if (this.error === 'SnowLuma进程无法终止') throw new Error(this.error)
    this.child = undefined; this.state = 'stopped'
  }
}
