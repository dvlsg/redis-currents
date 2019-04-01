import * as IORedis from 'ioredis'

import { deserialize } from './serialization'
import { Id, listener } from './types'

type Pending = {
  stream: string
  group: string
  consumer: string
  id: Id
  created: Date
  waiting: number
  deliveries: number
}

type PendingTuple = [Id, string, number, number]
type SerializedPair = [Id, [never, string]]
type DeserializedPair<T> = [Id, T]

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

const isNoGroupExistsError = (err: Error) => {
  return err.message.startsWith('NOGROUP')
}

const idToDate = (id: Id) => new Date(Number(id.split('-')[0]))

const deserializePair = <T>(pair: SerializedPair) => {
  const [id, arr] = pair
  const [, serialized] = arr
  const data = deserialize<T>(serialized)
  const paired: DeserializedPair<T> = [id, data]
  return paired
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

  public async ack(id: Id) {
    const result = await this.client.xack(this.stream, this.group, id)
    return result
  }

  public async pending(start = '-', end = '+', length = 100): Promise<Pending[]> {
    try {
      const { stream, group } = this
      const pendingTuples: PendingTuple[] = await this.client.xpending(stream, group, start, end, length)
      const pendingObjects = pendingTuples.map(tuple => {
        const [id, consumer, waiting, deliveries] = tuple
        const created = idToDate(id)
        const pending = { id, consumer, created, stream, group, deliveries, waiting }
        return pending
      })
      return pendingObjects
    } catch (err) {
      if (!isNoGroupExistsError(err)) {
        throw err
      }
      return []
    }
  }

  public async claim(ids: Id[]) {
    const MIN_IDLE_TIME = 0
    const pairs: SerializedPair[] = await this.client.xclaim(
      this.stream,
      this.group,
      this.consumer,
      MIN_IDLE_TIME,
      ...ids,
    )
    const mapped = pairs.map(pair => deserializePair<T>(pair))
    return mapped
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

  private async _read(fromId: Id) {
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
    const pairs = result[0][1] as SerializedPair[]
    const mapped = pairs.map(pair => deserializePair<T>(pair))
    return mapped
  }

  private async *_iterate() {
    await this._ensureGroup()
    while (true) {
      const fromId = this.checkBacklog ? '0-0' : '>'
      const pairs = await this._read(fromId)
      if (!pairs) {
        continue
      }
      if (doneWithBacklog(pairs)) {
        this.checkBacklog = false
      }
      for (const pair of pairs) {
        yield pair
      }
    }
  }
}

export default Consumer
export { Consumer }
