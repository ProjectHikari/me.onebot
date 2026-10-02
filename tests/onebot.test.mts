import assert from 'node:assert/strict'
import test from 'node:test'
import { createServer } from 'node:http'
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
