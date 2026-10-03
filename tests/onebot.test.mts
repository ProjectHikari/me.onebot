import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'node:http'
import { spawn } from 'node:child_process'
import { WebSocket, WebSocketServer } from 'ws'
import { OneBot, bodyMessage, validateConfig } from '../onebot/adapter.mts'
import { listen, until, closeServer } from './fixtures.mts'

test('OneBot segment and CQ messages preserve mentions and ignore self messages', () => {
  const message = bodyMessage({ post_type: 'message', self_id: 10, user_id: 20, message_id: 1, message_type: 'group', group_id: 30, message: '[CQ:at,qq=10]你好&#91;CQ:test&#93;' })!
  assert.equal(message.mentioned, true); assert.equal(message.text, '你好[CQ:test]'); assert.equal(message.groupId, '30')
  assert.equal(bodyMessage({ post_type: 'message', self_id: 10, user_id: 10, message_type: 'private' }), null)
})
test('forward WS authenticates, matches echo, deduplicates and releases timeout slots', async t => {
  const server = createServer(), wss = new WebSocketServer({ server }), endpoint = await listen(server)
  let remote: WebSocket | undefined, auth = ''
  wss.on('connection', (socket, request) => {
    remote = socket; auth = request.headers.authorization ?? ''
    socket.on('message', bytes => { const input = JSON.parse(bytes.toString()); if (input.action !== 'slow') socket.send(JSON.stringify({ status: 'ok', retcode: 0, echo: input.echo, data: { message_id: 42, message: input.params.message } })) })
  })
  const events: any[] = [], adapter = new OneBot(validateConfig({ mode: 'forward', url: endpoint.replace('http:', 'ws:'), token: 'test-secret', timeout: 100 }), async message => { events.push(message) })
  t.after(async () => { await adapter.close(); await new Promise<void>(resolve => wss.close(() => resolve())); await closeServer(server) })
  adapter.activate(); await until(() => remote, value => !!value)
  assert.equal(auth, 'Bearer test-secret')
  const payload = { post_type: 'message', message_type: 'private', self_id: 10, user_id: 20, message_id: 1, message: [{ type: 'text', data: { text: '你好' } }] }
  remote!.send(JSON.stringify(payload)); remote!.send(JSON.stringify(payload)); await until(() => events.length, length => length === 1)
  const reply = await adapter.service('send', { selfId: '10', userId: '20', text: '[CQ:at,qq=all]' }, new AbortController().signal)
  assert.equal(reply.message_id, 42); assert.deepEqual(reply.message, [{ type: 'text', data: { text: '[CQ:at,qq=all]' } }])
  await assert.rejects(adapter.service('action', { action: 'slow' }, new AbortController().signal), /超时/)
  assert.equal(adapter.pending.size, 0)
  remote!.close(); await until(() => adapter.reconnects, value => value > 0)
})
test('reverse WS rejects bad credentials and handles separate API/event clients', async t => {
  const events: any[] = [], adapter = new OneBot(validateConfig({ mode: 'reverse', port: 0, token: 'reverse-token' }), async message => { events.push(message) })
  await adapter.open(); adapter.activate()
  t.after(() => adapter.close())
  const address = (await adapter.service('status', {}, new AbortController().signal)).address
  const rejected = new WebSocket(address, { headers: { 'X-Self-ID': '10', Authorization: 'Bearer wrong' } })
  await new Promise<void>((resolve, reject) => { rejected.once('open', () => reject(new Error('invalid credentials accepted'))); rejected.once('error', () => resolve()) })
  const api = new WebSocket(`${address}/api`, { headers: { 'X-Self-ID': '10', Authorization: 'Bearer reverse-token', 'X-Client-Role': 'API' } })
  const event = new WebSocket(`${address}/event`, { headers: { 'X-Self-ID': '10', Authorization: 'Bearer reverse-token', 'X-Client-Role': 'Event' } })
  await Promise.all([api, event].map(socket => new Promise<void>((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject) })))
  api.on('message', bytes => { const value = JSON.parse(bytes.toString()); api.send(JSON.stringify({ echo: value.echo, status: 'ok', retcode: 0, data: { message_id: 99 } })) })
  event.send(JSON.stringify({ post_type: 'message', message_type: 'private', self_id: 10, user_id: 20, message_id: 12, message: 'hello' }))
  await until(() => events.length, count => count === 1)
  assert.equal((await adapter.service('send', { selfId: '10', userId: '20', text: '回复' }, new AbortController().signal)).message_id, 99)
  await adapter.close(); assert.equal(adapter.sockets.size, 0); assert.equal(adapter.work.active.size, 0); assert.equal(adapter.pending.size, 0)
})
test('heartbeat detects a silent reverse client and closes its actual socket', async t => {
  const adapter = new OneBot(validateConfig({ mode: 'reverse', port: 0, token: 'heartbeat-token' }), async () => {})
  await adapter.open(); adapter.activate(); t.after(() => adapter.close())
  const address = (await adapter.service('status', {}, new AbortController().signal)).address
  const client = new WebSocket(address, { autoPong: false, headers: { 'X-Self-ID': '10', Authorization: 'Bearer heartbeat-token' } })
  await new Promise<void>((resolve, reject) => { client.once('open', resolve); client.once('error', reject) })
  adapter.pulse(); adapter.pulse()
  await until(() => adapter.sockets.size, count => count === 0)
  assert.equal(adapter.api.size, 0)
})

test('connection updates preserve credentials, release old sockets and roll back an occupied listener', async t => {
  const { mkdtemp, readFile, writeFile, rm } = await import('node:fs/promises'), { tmpdir } = await import('node:os'), path = await import('node:path')
  const { OneBotManager } = await import('../onebot/manager.mts')
  const directory = await mkdtemp(path.join(tmpdir(), 'onebot-manager-')), file = path.join(directory, 'onebot.json')
  await writeFile(file, JSON.stringify({ mode: 'reverse', port: 0, token: 'private-token' }))
  const manager = new OneBotManager(file, async () => {}), signal = new AbortController().signal
  t.after(async () => { await manager.close(); await rm(directory, { recursive: true, force: true }) })
  await manager.open(); manager.activate()
  const before = await manager.service('status', {}, signal), config = await manager.service('configuration', {}, signal)
  assert.ok(!JSON.stringify(config).includes('private-token')); assert.equal(config.hasToken, true)
  const socket = new WebSocket(before.address, { headers: { 'X-Self-ID': '10', Authorization: 'Bearer private-token' } })
  await new Promise<void>((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject) })
  const old = manager.adapter!, blocked = createServer(); await listen(blocked); t.after(() => closeServer(blocked))
  await assert.rejects(manager.service('configure', { revision: config.revision, port: (blocked.address() as any).port }, signal), /EADDRINUSE/)
  assert.equal(old.sockets.size, 0); assert.equal(old.work.active.size, 0); assert.equal(manager.retired.size, 0)
  assert.equal(manager.adapter!.config.token, 'private-token'); assert.equal(JSON.parse(await readFile(file, 'utf8')).port, 0)
  const restored = manager.adapter
  const savedStartup = await manager.service('configure', { revision: config.revision, startup: { enabled: false, directory, timeout: 1000 } }, signal)
  assert.equal(manager.adapter, restored); assert.equal(savedStartup.restartRequired, true)
  const updated = await manager.service('configure', { revision: savedStartup.revision, token: 'next-token', port: 0 }, signal)
  assert.equal(updated.hasToken, true); assert.equal(manager.adapter!.config.token, 'next-token')
  await assert.rejects(manager.service('configure', { revision: config.revision, token: '' }, signal), /配置已变化/)
  await writeFile(file, '{"mode":"reverse","port":0,"token":"external"}')
  await assert.rejects(manager.service('configure', { revision: updated.revision, token: '' }, signal), /配置已变化/)
  assert.equal(manager.adapter!.config.token, 'next-token')
})

test('startup reuses a live OneBot without owning it and terminates only its own child', async t => {
  const { mkdtemp, writeFile, rm } = await import('node:fs/promises'), { tmpdir } = await import('node:os'), path = await import('node:path')
  const { SnowLumaStartup, startupConfig } = await import('../onebot/start.mts')
  const directory = await mkdtemp(path.join(tmpdir(), 'onebot-startup-')), server = createServer(), wss = new WebSocketServer({ server }), endpoint = (await listen(server)).replace('http:', 'ws:')
  wss.on('connection', socket => socket.on('message', bytes => { const request = JSON.parse(bytes.toString()); socket.send(JSON.stringify({ echo: request.echo, status: 'ok', retcode: 0, data: { user_id: 10 } })) }))
  t.after(async () => { await new Promise<void>(resolve => wss.close(() => resolve())); await closeServer(server); await rm(directory, { recursive: true, force: true }) })
  const reused = new SnowLumaStartup(startupConfig({ enabled: true, directory }))
  await reused.open({ mode: 'forward', url: endpoint, token: '' }, new AbortController().signal)
  assert.equal(reused.status().state, 'external'); assert.equal(reused.status().managed, false); await reused.close(); assert.ok(server.listening)
  await writeFile(path.join(directory, 'index.mjs'), 'setInterval(() => {}, 1000)')
  const owned = new SnowLumaStartup(startupConfig({ enabled: true, directory })); t.after(() => owned.close())
  await owned.open({ mode: 'reverse', url: endpoint, token: '' }, new AbortController().signal)
  const pid = owned.status().pid; assert.ok(pid); await owned.close()
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' }); assert.equal(owned.status().state, 'stopped')
})

test('startup restarts a registered SnowLuma process after an authorization rejection', async t => {
  const { mkdtemp, writeFile, rm } = await import('node:fs/promises'), { tmpdir } = await import('node:os'), path = await import('node:path')
  const { SnowLumaStartup, startupConfig } = await import('../onebot/start.mts')
  const directory = await mkdtemp(path.join(tmpdir(), 'onebot-restart-')), server = createServer()
  server.on('upgrade', (_request, socket) => socket.end('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'))
  const endpoint = (await listen(server)).replace('http:', 'ws:')
  await writeFile(path.join(directory, 'index.mjs'), 'setInterval(() => {}, 1000)')
  const previous = spawn(process.execPath, ['index.mjs'], { cwd: directory, stdio: 'ignore' })
  const previousExit = new Promise<void>((resolve, reject) => { previous.once('exit', () => resolve()); previous.once('error', reject) })
  await new Promise<void>((resolve, reject) => { previous.once('spawn', resolve); previous.once('error', reject) })
  await writeFile(path.join(directory, 'runtime.pid'), String(previous.pid))
  const startup = new SnowLumaStartup(startupConfig({ enabled: true, directory, timeout: 1000 }))
  t.after(async () => { await startup.close(); previous.kill('SIGKILL'); await closeServer(server); await rm(directory, { recursive: true, force: true }) })
  await startup.open({ mode: 'forward', url: endpoint, token: 'test-token' }, new AbortController().signal)
  await previousExit
  assert.equal(startup.status().managed, true)
  assert.equal(startup.status().state, 'running')
})
