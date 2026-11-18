import path from 'node:path'
import { run } from '../../core/bridge/sdk/typescript/sdk.mts'
import { dataRoot, log } from '../shared/runtime.mts'
import { OneBotManager } from './manager.mts'

await run(async ctx => {
  const adapter = new OneBotManager(process.env.ME_ONEBOT_CONFIG ?? path.join(dataRoot, 'onebot.json'), (message, signal) => ctx.parallel('body.onebot.message', message as any, signal), ctx.signal,
    (event, signal) => ctx.parallel('body.onebot.verification', event, signal))
  await ctx.defer('OneBot连接与消息调用', () => adapter.close())
  await adapter.open()
  log('INFO', ctx.pluginId, `OneBot连接方式: ${adapter.adapter!.config.mode === 'forward' ? '正向' : '反向'}WebSocket`)
  await ctx.provide('adapter.onebot', (method, input, signal) => adapter.service(method, input, signal))
  await ctx.every('OneBot连接心跳', 20000, async () => adapter.pulse())
  await ctx.after('启用OneBot入口', 0, async () => adapter.activate())
})
