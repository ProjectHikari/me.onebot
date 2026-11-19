import path from 'node:path'
import { RemoteError, run, type JSONValue } from '../../core/bridge/sdk/typescript/sdk.mts'
import { dataRoot, log } from '../shared/runtime.mts'
import { commandEvent, commandGroups, OneBotCommands } from './commands.mts'
import { OneBotManager } from './manager.mts'

await run(async ctx => {
  const adapter = new OneBotManager(process.env.ME_ONEBOT_CONFIG ?? path.join(dataRoot, 'onebot.json'), async (message, signal) => {
    if (!await commands.handle(message, signal)) {
      await ctx.parallel('body.onebot.message', message as unknown as JSONValue, signal)
    }
  }, ctx.signal, (event, signal) => ctx.parallel('body.onebot.verification', event, signal))
  let commandsDeclared = true
  const commands = new OneBotCommands(async (event, signal) => {
    if (!commandsDeclared) return
    try {
      await ctx.parallel(commandEvent, event as unknown as JSONValue, signal)
    } catch (error) {
      if (!(error instanceof RemoteError) || error.message !== `service is not declared by plugin: event ${commandEvent}`) throw error
      commandsDeclared = false
      log('WARN', ctx.pluginId, `当前配置缺少 ${commandEvent}，更新插件声明并重启后可显示插件命令`)
    }
  }, ({ selfId, userId, groupId }, text, signal) =>
    adapter.service('send', { selfId, userId, groupId, text }, signal), commandGroups(process.env.ME_ONEBOT_COMMAND_GROUPS))
  await ctx.defer('OneBot连接与消息调用', () => adapter.close())
  await adapter.open()
  log('INFO', ctx.pluginId, `OneBot连接方式: ${adapter.adapter!.config.mode === 'forward' ? '正向' : '反向'}WebSocket`)
  await ctx.provide('adapter.onebot', async (method, input, signal) =>
    method === 'commands.contribute' ? commands.contribute(input) : adapter.service(method, input, signal))
  await ctx.every('OneBot连接心跳', 20000, async () => adapter.pulse())
  await ctx.after('启用OneBot入口', 0, async () => adapter.activate())
})
