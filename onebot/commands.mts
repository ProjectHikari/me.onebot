import { randomUUID } from 'node:crypto'
import type { Context, JSONValue } from '../../core/bridge/sdk/typescript/sdk.mts'
import { log } from '../shared/runtime.mts'
import type { BodyMessage } from './adapter.mts'

export const commandEvent = 'body.onebot.commands'
const commandName = /^[a-z][a-z0-9-]{0,31}$/

export function commandGroups(raw: string | undefined): ReadonlySet<string> | undefined {
  if (raw === undefined) return
  const groups = raw.trim() ? raw.split(',').map(group => group.trim()) : []
  if (groups.some(group => !/^[1-9][0-9]{0,14}$/.test(group))) throw new Error('OneBot 命令群号格式无效')
  return new Set(groups)
}

export type CommandHelp = {
  name: string
  usage: string
  description: string
  category: string
  scope: 'group' | 'private' | 'both'
}
type Command = CommandHelp & { execute(message: BodyMessage, signal: AbortSignal): Promise<void> }
type Discover = { phase: 'discover'; requestId: string }
type Execute = { phase: 'execute'; binding: string; name: string; message: BodyMessage }
type Dispatch = (event: Discover | Execute, signal: AbortSignal) => Promise<void>
type Entry = { binding: string; help: CommandHelp }

export async function declareCommands(ctx: Pick<Context, 'on' | 'call'>, commands: readonly Command[]): Promise<void> {
  const binding = randomUUID()
  const entries = commands.map(({ execute, ...help }) => help)
  if (!entries.length || new Set(entries.map(entry => entry.name)).size !== entries.length || !entries.every(validHelp)) {
    throw new Error('命令声明无效')
  }
  await ctx.on(commandEvent, async (payload, signal) => {
    const event = payload as unknown as Discover | Execute
    if (event.phase === 'discover') {
      await ctx.call('adapter.onebot', 'commands.contribute', { requestId: event.requestId, binding, entries }, signal)
    } else if (event.phase === 'execute' && event.binding === binding) {
      await commands.find(command => command.name === event.name)?.execute(event.message, signal)
    }
  })
}

export function sendCommandText(ctx: Pick<Context, 'call'>, message: BodyMessage, text: string, signal: AbortSignal): Promise<JSONValue> {
  const { selfId, userId, groupId } = message
  return ctx.call('adapter.onebot', 'send', { selfId, userId, ...(groupId ? { groupId } : {}), text }, signal)
}

function validHelp(raw: unknown): raw is CommandHelp {
  if (!raw || typeof raw !== 'object') return false
  const { name, usage, description, category, scope } = raw as CommandHelp
  const singleLine = (value: unknown, max: number) =>
    typeof value === 'string' && value.length <= max && !!value.trim() && !/[\r\n]/.test(value)
  return typeof name === 'string' && commandName.test(name) && name !== 'help' &&
    singleLine(usage, 160) && singleLine(description, 160) && singleLine(category, 40) &&
    (scope === 'group' || scope === 'private' || scope === 'both')
}

const inScope = (help: CommandHelp, message: BodyMessage) =>
  help.scope === 'both' || help.scope === (message.groupId ? 'group' : 'private')

export class OneBotCommands {
  private requests = new Map<string, Entry[]>()

  private readonly dispatch: Dispatch
  private readonly send: (message: BodyMessage, text: string, signal: AbortSignal) => Promise<unknown>
  private readonly groups?: ReadonlySet<string>

  constructor(dispatch: Dispatch, send: OneBotCommands['send'], groups?: ReadonlySet<string>) {
    this.dispatch = dispatch
    this.send = send
    this.groups = groups
  }

  contribute(input: unknown): null {
    if (!input || typeof input !== 'object') throw new Error('命令声明无效')
    const { requestId, binding, entries } = input as { requestId: string; binding: string; entries: unknown }
    const pending = this.requests.get(requestId)
    if (!pending) return null
    if (typeof binding !== 'string' || !binding || !Array.isArray(entries) || !entries.every(validHelp)) {
      throw new Error('命令声明无效')
    }
    for (const help of entries) pending.push({ binding, help })
    return null
  }

  async handle(message: BodyMessage, signal: AbortSignal): Promise<boolean> {
    if (this.groups && (!message.groupId || !this.groups.has(message.groupId))) return false
    const firstContent = message.segments.findIndex(segment => segment.type !== 'text')
    const leadingText = (firstContent < 0 ? message.segments : message.segments.slice(0, firstContent))
      .map(segment => String(segment.data?.text ?? '')).join('')
    const name = /^\s*\/([a-z][a-z0-9-]{0,31})(?=\s|$)/.exec(leadingText)?.[1]
    if (!name) return false

    const requestId = randomUUID()
    const entries: Entry[] = []
    let incomplete = false
    this.requests.set(requestId, entries)
    try {
      await this.dispatch({ phase: 'discover', requestId }, AbortSignal.any([signal, AbortSignal.timeout(2000)]))
    } catch (error) {
      signal.throwIfAborted()
      incomplete = true
      log('WARN', 'me.onebot', `命令查询失败 - ${String(error)}`)
    } finally {
      this.requests.delete(requestId)
    }
    signal.throwIfAborted()

    if (name === 'help') {
      await this.send(message, this.menu(entries, message), signal)
      return true
    }

    const named = entries.filter(entry => entry.help.name === name)
    const available = named.filter(entry => inScope(entry.help, message))
    if (incomplete) {
      if (!named.length) return false
      await this.send(message, '暂时无法确认命令，请稍后重试', signal)
    } else if (available.length > 1) {
      log('ERROR', 'me.onebot', `命令声明冲突 - /${name}`)
      await this.send(message, '命令暂不可用', signal)
    } else if (available.length === 1) {
      const { binding } = available[0]
      await this.dispatch({ phase: 'execute', binding, name, message }, signal)
    } else if (named.length) {
      await this.send(message, message.groupId ? '请在私聊使用此命令' : '请在群聊使用此命令', signal)
    } else {
      return false
    }
    return true
  }

  private menu(entries: Entry[], message: BodyMessage): string {
    const visible = entries.filter(entry => inScope(entry.help, message))
    const counts = new Map<string, number>()
    for (const { help } of visible) counts.set(help.name, (counts.get(help.name) ?? 0) + 1)
    const categories = new Map<string, string[]>()
    for (const { help } of visible.sort((a, b) => a.help.name.localeCompare(b.help.name))) {
      if (counts.get(help.name) !== 1) continue
      const lines = categories.get(help.category) ?? []
      lines.push(`${help.usage} - ${help.description}`)
      categories.set(help.category, lines)
    }
    const sections = ['可用命令\n/help - 查看帮助']
    for (const [category, lines] of [...categories].sort(([a], [b]) => a.localeCompare(b, 'zh-CN'))) {
      sections.push([category, ...lines].join('\n'))
    }
    return sections.join('\n\n')
  }
}
