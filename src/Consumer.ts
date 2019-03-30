import * as IORedis from 'ioredis'

import { deserialize } from './serialization'
import { listener } from './types'

type ConsumerOptions = {
  block?: number
  count?: number
  from?: string
}

const defaultOptions: Required<ConsumerOptions> = {
  block: 0,
  count: 1,
  from: '0',
}

const doneWithBacklog = <T>(items: T[]) => {
  return items.length === 0
}

const isGroupAlreadyExistsError = (err: Error) => {
  return err.message.startsWith('BUSYGROUP')
}

class Consumer<T = any> {
  private readonly client: IORedis.Redis
  private readonly block: number
  private readonly count: number
  private readonly from: string
  private checkBacklog: boolean = true

  constructor(client: IORedis.Redis, stream: string, group: string, consumer: string)
  constructor(client: IORedis.Redis, stream: string, group: string, consumer: string, opts: ConsumerOptions)
  constructor(uri: string, stream: string, group: string, consumer: string)
  constructor(uri: string, stream: string, group: string, consumer: string, opts: ConsumerOptions)
  constructor(
    clientOpts: IORedis.Redis | string,
    private stream: string,
    private group: string,
    private consumer: string,
    opts: ConsumerOptions = defaultOptions,
  ) {
    this.client = typeof clientOpts === 'string' ? new IORedis(clientOpts) : clientOpts
    this.block = opts.block || defaultOptions.block
    this.count = opts.count || defaultOptions.count
    this.from = opts.from || defaultOptions.from
  }

  public [Symbol.asyncIterator]() {
    return this._iterate()
  }

  public disconnect() {
    this.client.disconnect()
  }

  public on(name: string, callback: listener) {
    return this.client.on(name, callback)
  }

  public async ack(id: string) {
    const result = await this.client.xack(this.stream, this.group, id)
    return result
  }

  private async _ensureGroup() {
    try {
      await this.client.xgroup('CREATE', this.stream, this.group, this.from, 'MKSTREAM')
    } catch (err) {
      if (!isGroupAlreadyExistsError(err)) {
        throw err
      }
    }
  }

  private async _read(fromId: string) {
    const result = await this.client.xreadgroup(
      'GROUP',
      this.group,
      this.consumer,
      'BLOCK',
      this.block,
      'COUNT',
      this.count,
      'STREAMS',
      this.stream,
      fromId,
    )
    if (!result) {
      return null
    }
    const items = result[0][1] as Item[]

    type Item = [string, [string, string]]

    const mapped = items.map((item: Item) => {
      const [id, arr] = item
      const [, serialized] = arr
      const paired: [string, string] = [id, serialized]
      return paired
    })
    return mapped
  }

  private async *_iterate() {
    await this._ensureGroup()
    while (true) {
      const fromId = this.checkBacklog ? '0-0' : '>'
      const items = await this._read(fromId)
      if (!items) {
        continue
      }
      if (doneWithBacklog(items)) {
        this.checkBacklog = false
      }
      for (const [id, serialized] of items) {
        const data = deserialize<T>(serialized)
        const tuple: [string, T] = [id, data]
        yield tuple
      }
    }
  }
}

export default Consumer
export { Consumer }
