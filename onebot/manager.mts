import { readFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { log, object, Work, writeJSON } from '../shared/runtime.mts'
import { SnowLumaStartup } from './start.mts'
import { OneBot, validateConfig, type OneBotConfig, type VerificationIngress } from './adapter.mts'

function publicURL(raw: string): string { const url = new URL(raw); for (const key of url.searchParams.keys()) if (/token|key|secret|password|authorization/i.test(key)) url.searchParams.set(key, '[已隐藏]'); return url.href }
const digest = (value: string) => createHash('sha256').update(value).digest('hex')
async function contents(file: string): Promise<string> {
  try { return await readFile(file, 'utf8') } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return ''; throw error }
}
export class OneBotManager {
  work: Work
  file: string
  adapter?: OneBot
  startup?: SnowLumaStartup
  retired = new Set<OneBot>()
  revision = ''
  mutations = Promise.resolve()
  enabled = false
  updating = false
  readonly onVerification?: VerificationIngress
  constructor(file: string, onMessage: OneBot['onMessage'], signal?: AbortSignal, onVerification?: VerificationIngress) {
    this.file = file; this.onMessage = onMessage; this.onVerification = onVerification; this.work = new Work(signal)
  }
  onMessage: OneBot['onMessage']
  create(config: OneBotConfig): OneBot { const value = new OneBot(config, this.onMessage, this.work.signal, this.onVerification); this.retired.add(value); return value }
  async open(): Promise<void> {
    const saved = await contents(this.file)
    this.revision = digest(saved); this.adapter = this.create(validateConfig(saved ? JSON.parse(saved) : {}))
    this.startup = new SnowLumaStartup(this.adapter.config.startup)
    await this.startup.open(this.adapter.config, this.work.signal)
    await this.adapter.open(); this.retired.delete(this.adapter)
  }
  activate(): void { this.work.signal.throwIfAborted(); this.enabled = true; this.adapter?.activate() }
  pulse(): void { if (!this.updating) this.adapter?.pulse() }
  configuration(): any {
    if (!this.adapter) throw new Error('OneBot尚未初始化')
    const { token, ...config } = this.adapter.config
    return { ...config, url: publicURL(config.url), restartRequired: JSON.stringify(config.startup) !== JSON.stringify(this.startup?.config), hasToken: !!token, revision: this.revision }
  }
  async configure(raw: any, caller: AbortSignal): Promise<any> {
    return this.work.run(async lifetime => {
      const signal = AbortSignal.any([lifetime, caller]), pending = this.mutations.then(async () => {
        signal.throwIfAborted()
        if (!this.adapter || this.retired.size) throw new Error('OneBot清理尚未完成')
        const input = object(raw), previous = this.adapter, revision = digest(await contents(this.file))
        if (revision !== this.revision || input.revision !== this.revision) throw new Error('连接配置已变化，请重新加载后保存')
        const next = validateConfig({ ...previous.config, ...input, url: input.url === publicURL(previous.config.url) ? previous.config.url : input.url ?? previous.config.url, token: input.token === undefined ? previous.config.token : input.token })
        if (JSON.stringify(next) === JSON.stringify(previous.config)) return this.configuration()
        const { startup: oldStartup, ...oldTransport } = previous.config, { startup: newStartup, ...newTransport } = next
        if (JSON.stringify(oldTransport) === JSON.stringify(newTransport)) {
          signal.throwIfAborted()
          if (digest(await contents(this.file)) !== revision) throw new Error('连接配置已被外部修改')
          await writeJSON(this.file, next); previous.config = next; this.revision = digest(JSON.stringify(next, null, 2) + '\n')
          log('OK', 'me.onebot', '启动配置已保存 - 重新加载后生效'); return this.configuration()
        }
        this.updating = true
        this.retired.add(previous)
        let fresh: OneBot | undefined
        try {
          // 同端口不能同时监听，旧连接实际退出后才创建新入口
          await previous.close(); this.retired.delete(previous)
          signal.throwIfAborted()
          fresh = this.create(next); await fresh.open(); signal.throwIfAborted()
          if (digest(await contents(this.file)) !== revision) throw new Error('连接配置已被外部修改')
          await writeJSON(this.file, next)
          this.revision = digest(JSON.stringify(next, null, 2) + '\n')
          this.adapter = fresh; this.retired.delete(fresh)
          if (this.enabled && !this.work.signal.aborted) fresh.activate()
          log('OK', 'me.onebot', '连接配置已更新'); return this.configuration()
        } catch (error) {
          if (fresh) { await fresh.close(); this.retired.delete(fresh) }
          if (!this.retired.size && !this.work.signal.aborted) {
            const restored = this.create(previous.config)
            try { await restored.open(); this.adapter = restored; this.retired.delete(restored); if (this.enabled) restored.activate() }
            catch (restoreError) { log('ERROR', 'me.onebot', '原连接恢复失败'); throw new AggregateError([error, restoreError], '连接更新失败，原连接无法恢复') }
          }
          log('FAILED', 'me.onebot', '连接配置未更新'); throw error
        } finally { this.updating = false }
      })
      this.mutations = pending.then(() => {}, () => {}); return pending
    })
  }
  async service(method: string, input: any, signal: AbortSignal): Promise<any> {
    this.work.signal.throwIfAborted()
    if (method === 'configuration') return this.configuration()
    if (method === 'configure') return this.configure(input, signal)
    if (method === 'status') return { ...await this.adapter!.service(method, input, signal), startup: this.startup?.status() }
    if (this.updating && method !== 'status') throw new Error('OneBot正在更新连接')
    return this.adapter!.service(method, input, signal)
  }
  async close(): Promise<void> {
    this.enabled = false; this.work.controller.abort(); await this.mutations
    if (this.adapter) this.retired.add(this.adapter)
    const results = await Promise.allSettled([...this.retired].map(async value => { await value.close(); this.retired.delete(value) }))
    const startup = await Promise.allSettled([this.startup?.close()]); results.push(...startup)
    await this.work.close()
    const errors = results.filter((value): value is PromiseRejectedResult => value.status === 'rejected').map(value => value.reason)
    if (errors.length) throw new AggregateError(errors, 'OneBot连接未完整释放')
  }
}
