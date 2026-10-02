import { createServer, type Server } from 'node:http'
import { randomUUID, timingSafeEqual } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { WebSocket, WebSocketServer } from 'ws'
import { integer, log, object, text, VERSION, Work } from '../shared/runtime.mts'

export type OneBotConfig = { mode: 'forward' | 'reverse'; url: string; host: string; port: number; path: string; token: string; timeout: number }
export type BodyMessage = { selfId: string; userId: string; groupId?: string; messageId: string; text: string; mentioned: boolean; segments: any[]; time: number }
type Pending = { socket: WebSocket; resolve: (value: any) => void; reject: (error: Error) => void; release: () => void }

export function validateConfig(raw: any): OneBotConfig {
  const input = object(raw), mode = input.mode ?? 'forward'
  if (!['forward', 'reverse'].includes(mode)) throw new Error('OneBot模式需要是forward或reverse')
  const url = text(input.url ?? 'ws://127.0.0.1:6700/', 'WebSocket地址', 2048)
  const parsed = new URL(url)
  if (!['ws:', 'wss:'].includes(parsed.protocol) || parsed.username || parsed.password) throw new Error('WebSocket地址无效')
  const host = text(input.host ?? '127.0.0.1', '监听地址', 256), token = typeof input.token === 'string' ? input.token : ''
  if (/[\r\n]/.test(token) || token.length > 4096) throw new Error('OneBot令牌格式错误')
  if (mode === 'reverse' && !['127.0.0.1', '::1', 'localhost'].includes(host) && !token) throw new Error('远程反向WebSocket需要设置令牌')
  const route = input.path ?? '/onebot'
  if (typeof route !== 'string' || !/^\/[a-zA-Z0-9/_-]*$/.test(route)) throw new Error('反向WebSocket路径无效')
  return { mode, url, host, port: integer(input.port ?? 6701, '端口', 0, 65535), path: route, token, timeout: integer(input.timeout ?? 15000, '调用超时', 100, 25000) }
}
export function bodyMessage(raw: any): BodyMessage | null {
  const value = object(raw)
  if (value.post_type !== 'message' || !['private', 'group'].includes(value.message_type) || String(value.user_id) === String(value.self_id)) return null
  if (!value.self_id || !value.user_id || value.message_id === undefined) throw new Error('OneBot消息缺少身份字段')
  const segments = Array.isArray(value.message) ? value.message : typeof value.message === 'string' ? cqSegments(value.message) : []
  let content = '', mentioned = false
  for (const segment of segments) {
    if (segment.type === 'text') content += typeof segment.data?.text === 'string' ? segment.data.text : ''
    if (segment.type === 'at' && String(segment.data?.qq) === String(value.self_id)) mentioned = true
  }
  return { selfId: String(value.self_id), userId: String(value.user_id), groupId: value.message_type === 'group' ? text(String(value.group_id), '群ID', 32) : undefined,
    messageId: String(value.message_id), text: content.slice(0, 32768), mentioned, segments, time: Number(value.time) || Math.floor(Date.now() / 1000) }
}
function cqSegments(content: string): any[] {
  const decode = (value: string) => value.replace(/&#91;/g, '[').replace(/&#93;/g, ']').replace(/&#44;/g, ',').replace(/&amp;/g, '&')
  const result: any[] = []; let position = 0
  for (const match of content.matchAll(/\[CQ:([a-zA-Z0-9_]+)((?:,[^\]]*)?)\]/g)) {
    if (match.index! > position) result.push({ type: 'text', data: { text: decode(content.slice(position, match.index)) } })
    const data: Record<string, string> = {}
    for (const field of match[2].split(',').slice(1)) { const end = field.indexOf('='); if (end >= 0) data[field.slice(0, end)] = decode(field.slice(end + 1)) }
    result.push({ type: match[1], data }); position = match.index! + match[0].length
  }
  if (position < content.length) result.push({ type: 'text', data: { text: decode(content.slice(position)) } })
  return result
}

export class OneBot {
  config: OneBotConfig
  work: Work
  server?: Server
  wss?: WebSocketServer
  sockets = new Set<WebSocket>()
  api = new Map<string, WebSocket>()
  pending = new Map<string, Pending>()
  seen = new Map<string, number>()
  alive = new WeakMap<WebSocket, boolean>()
  enabled = false
  stopping = false
  received = 0
  sent = 0
  reconnects = 0
  lastError = ''
  onMessage: (message: BodyMessage, signal: AbortSignal) => Promise<void>
  constructor(config: OneBotConfig, onMessage: OneBot['onMessage'], signal?: AbortSignal) { this.config = config; this.onMessage = onMessage; this.work = new Work(signal) }
  async open(): Promise<void> {
    if (this.config.mode !== 'reverse') return
    this.server = createServer((_req, res) => { res.writeHead(404); res.end() })
    this.wss = new WebSocketServer({ noServer: true, maxPayload: 524288 })
    this.server.on('upgrade', (request, socket, head) => {
      const url = new URL(request.url ?? '/', 'http://localhost'), token = request.headers.authorization?.replace(/^Bearer /, '') ?? url.searchParams.get('access_token') ?? ''
      const expected = Buffer.from(this.config.token), supplied = Buffer.from(token)
      const authorized = !expected.length || (expected.length === supplied.length && timingSafeEqual(expected, supplied))
      const allowed = [this.config.path, `${this.config.path}/api`, `${this.config.path}/event`].includes(url.pathname)
      if (!this.enabled || !authorized || !allowed || this.sockets.size >= 16) { socket.end('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n'); return }
      const selfId = String(request.headers['x-self-id'] ?? '')
      if (!/^[0-9]{1,32}$/.test(selfId)) { socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n'); return }
      const role = String(request.headers['x-client-role'] ?? (url.pathname.endsWith('/event') ? 'Event' : url.pathname.endsWith('/api') ? 'API' : 'Universal')).toLowerCase()
      if (!['api', 'event', 'universal'].includes(role)) { socket.end('HTTP/1.1 400 Bad Request\r\n\r\n'); return }
      if (role !== 'event' && this.api.has(selfId)) { socket.end('HTTP/1.1 409 Conflict\r\n\r\n'); return }
      this.wss!.handleUpgrade(request, socket, head, ws => this.attach(ws, selfId, role))
    })
    await new Promise<void>((resolve, reject) => { this.server!.once('error', reject); this.server!.listen(this.config.port, this.config.host, () => { this.server!.off('error', reject); resolve() }) })
    this.server.on('error', error => { this.lastError = error.message; log('ERROR', 'me.onebot', error.message) })
  }
  activate(): void {
    this.enabled = true
    if (this.config.mode === 'forward') this.work.run(signal => this.connect(signal)).catch(error => { if (!this.work.signal.aborted) log('ERROR', 'me.onebot', String(error)) })
  }
  async connect(signal: AbortSignal): Promise<void> {
    let delay = 500
    while (!signal.aborted) {
      const socket = new WebSocket(this.config.url, { headers: this.config.token ? { Authorization: `Bearer ${this.config.token}` } : {}, handshakeTimeout: 10000, maxPayload: 524288 })
      this.attach(socket, '', 'universal')
      let opened = false
      await new Promise<void>(resolve => {
        const abort = () => socket.terminate()
        signal.addEventListener('abort', abort, { once: true })
        if (signal.aborted) abort()
        socket.once('open', () => { opened = true; delay = 500; this.lastError = ''; log('OK', 'me.onebot', 'OneBot连接已建立') })
        socket.once('close', () => { signal.removeEventListener('abort', abort); resolve() })
      })
      if (signal.aborted) return
      this.reconnects++
      log('WARN', 'me.onebot', `${opened ? 'OneBot连接断开' : 'OneBot连接未建立'} - ${delay}ms后重试`)
      await sleep(delay, undefined, { signal }).catch(() => {})
      if (!opened) delay = Math.min(delay * 2, 30000)
    }
  }
  attach(socket: WebSocket, selfId: string, role: string): void {
    this.sockets.add(socket)
    this.alive.set(socket, true)
    socket.on('pong', () => this.alive.set(socket, true))
    if (socket.readyState === WebSocket.OPEN) this.lastError = ''
    if (role !== 'event') this.api.set(selfId, socket)
    socket.on('error', error => { this.lastError = error.message })
    socket.on('close', () => {
      this.sockets.delete(socket)
      for (const [id, current] of this.api) if (current === socket) this.api.delete(id)
      for (const [id, pending] of this.pending) if (pending.socket === socket) { pending.release(); this.pending.delete(id); pending.reject(new Error('OneBot连接已关闭，发送结果未知')) }
    })
    socket.on('message', bytes => {
      if (!this.enabled || this.stopping) return
      try {
        const value = JSON.parse(bytes.toString())
        if (value.echo !== undefined) {
          const id = String(value.echo), pending = this.pending.get(id)
          if (!pending || pending.socket !== socket) return
          pending.release(); this.pending.delete(id)
          if (value.status === 'ok' && value.retcode === 0) pending.resolve(value.data)
          else pending.reject(new Error(`OneBot调用失败，retcode=${value.retcode}`))
          return
        }
        if (value.self_id) {
          if (selfId && String(value.self_id) !== selfId) throw new Error('OneBot上报身份与连接不一致')
          if (!selfId && role !== 'event') { selfId = String(value.self_id); this.api.set(selfId, socket) }
        }
        const message = bodyMessage(value)
        if (!message) return
        const key = `${message.selfId}:${message.groupId ?? message.userId}:${message.messageId}`, now = Date.now()
        for (const [key, time] of this.seen) if (now - time > 120000) this.seen.delete(key)
        if (this.seen.has(key)) return
        if (this.seen.size >= 10000) this.seen.delete(this.seen.keys().next().value!)
        this.seen.set(key, now); this.received++
        if (this.work.active.size >= 64) { log('WARN', 'me.onebot', '消息处理已满，本条消息未接入'); return }
        this.work.run(signal => this.onMessage(message, signal)).catch(error => { if (!this.work.signal.aborted) log('WARN', 'me.onebot', `消息处理失败: ${String(error)}`) })
      } catch (error) { this.lastError = String(error); log('WARN', 'me.onebot', '收到无效的OneBot数据') }
    })
  }
  pulse(): void {
    if (!this.enabled || this.stopping) return
    for (const socket of this.sockets) {
      if (socket.readyState !== WebSocket.OPEN) continue
      if (!this.alive.get(socket)) { socket.terminate(); continue }
      this.alive.set(socket, false)
      socket.ping(undefined, undefined, error => { if (error) socket.terminate() })
    }
  }
  action(raw: any, signal: AbortSignal): Promise<any> {
    const input = object(raw), selfId = input.selfId ? text(input.selfId, 'Bot ID', 32) : ''
    const socket = this.api.get(selfId) ?? (this.config.mode === 'forward' ? this.api.get('') : undefined)
    if (!socket || socket.readyState !== WebSocket.OPEN || this.stopping) throw new Error('OneBot尚未连接')
    if (this.pending.size >= 64) throw new Error('OneBot调用已满')
    const action = text(input.action, '动作', 128), params = object(input.params ?? {}), id = randomUUID()
    signal.throwIfAborted()
    return new Promise((resolve, reject) => {
      const finish = (error: Error) => { const pending = this.pending.get(id); if (!pending) return; pending.release(); this.pending.delete(id); reject(error) }
      const abort = () => finish(new Error('OneBot调用已取消，发送结果未知'))
      const timer = setTimeout(() => finish(new Error('OneBot调用超时，发送结果未知')), this.config.timeout)
      const release = () => { clearTimeout(timer); signal.removeEventListener('abort', abort) }
      this.pending.set(id, { socket, resolve, reject, release }); signal.addEventListener('abort', abort, { once: true })
      socket.send(JSON.stringify({ action, params, echo: id }), error => { if (error) finish(error) })
    })
  }
  async service(method: string, raw: any, signal: AbortSignal): Promise<any> {
    this.work.signal.throwIfAborted()
    const input = raw ?? {}
    if (method === 'status') return { version: VERSION, mode: this.config.mode, connected: [...new Set(this.api.values())].filter(socket => socket.readyState === WebSocket.OPEN).length,
      bots: [...this.api.keys()].filter(Boolean), received: this.received, sent: this.sent, pending: this.pending.size, reconnects: this.reconnects, error: this.lastError,
      address: this.config.mode === 'reverse' ? `ws://${this.config.host}:${(this.server?.address() as any)?.port ?? this.config.port}${this.config.path}` : this.config.url.replace(/\?.*$/, '') }
    if (method === 'action') return this.action(input, AbortSignal.any([signal, this.work.signal]))
    if (method === 'send') {
      const target = input.groupId ?? input.userId
      if (!/^[0-9]{1,16}$/.test(String(target)) || !Number.isSafeInteger(Number(target)) || Number(target) <= 0) throw new Error('发送目标无效')
      const content = text(input.text, '回复文本', 32768)
      const result = await this.action({ selfId: input.selfId, action: input.groupId ? 'send_group_msg' : 'send_private_msg',
        params: { [input.groupId ? 'group_id' : 'user_id']: Number(target), message: [{ type: 'text', data: { text: content } }] } }, AbortSignal.any([signal, this.work.signal]))
      this.sent++; return result
    }
    throw new Error(`未知OneBot方法${method}`)
  }
  async close(): Promise<void> {
    this.stopping = true; this.enabled = false; this.work.controller.abort()
    for (const pending of this.pending.values()) { pending.release(); pending.reject(new Error('OneBot正在停止，发送结果未知')) }
    this.pending.clear()
    const sockets = [...this.sockets]
    await Promise.all(sockets.map(socket => new Promise<void>(resolve => {
      if (socket.readyState === WebSocket.CLOSED) { resolve(); return }
      const timer = setTimeout(() => socket.terminate(), 500)
      socket.once('close', () => { clearTimeout(timer); resolve() }); socket.close()
    })))
    if (this.wss) await new Promise<void>(resolve => this.wss!.close(() => resolve()))
    if (this.server?.listening) await new Promise<void>((resolve, reject) => this.server!.close(error => error ? reject(error) : resolve()))
    await this.work.close()
  }
}
