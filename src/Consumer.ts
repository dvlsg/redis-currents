import * as IORedis from 'ioredis'

import { deserialize } from './serialization'
import { Id, listener } from './types'

type Pending = {
  /**
   * The stream the pending message is in.
   */
  stream: string

  /**
   * The group the pending message is in.
   */
  group: string

  /**
   * The consumer the pending message was last delievered to.
   */
  consumer: string

  /**
   * The id of the pending message.
   */
  id: Id

  /**
   * The date on which the pending message was written to the stream.
   */
  created: Date

  /**
   * The amount of time the pending message has been unacked.
   */
  waiting: number

  /**
   * The number of times the pending message has been delivered so far.
   */
  deliveries: number
}

type PendingTuple = [Id, string, number, number]
type SerializedPair = [Id, [never, string]]
type DeserializedPair<T> = [Id, T]

type ConsumerOptions = {
  /**
   * The amount of time in milliseconds which the Redis client should block before timing out.
   * When a timeout occurs, the `Consumer` will loop back around and re-initiate a group read.
   *
   * Set to `'0'` to never timeout.
   *
   * Note that any given Redis client using a blocking operation like `XREADGROUP`
   * will block all other commands from being issued on the same connection while it is pending.
   */
  block?: number

  /**
   * The max number of messages to deliver for each read.
   */
  count?: number

  /**
   * The id of the message to start consuming from.
   *
   * Pass `'0'` to begin from the start of the data in the stream.
   * Pass `'$'` to begin with new messages written to the stream.
   */
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

const isConnectionClosedError = (err: Error) => {
  return err.message === 'Connection is closed.'
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
  private running: boolean = true
  private checkBacklog: boolean = true
  private reading: boolean = false

  /**
   * Creates a new Consumer.
   *
   * @param uri The uri of the Redis server.
   * @param stream The name of the stream to read from.
   * @param group The name of the group to attach to.
   * @param consumer The name of the consumer to identify as.
   */
  constructor(uri: string, stream: string, group: string, consumer: string)

  /**
   * Creates a new Consumer.
   *
   * @param uri The uri of the Redis server.
   * @param stream The name of the stream to read from.
   * @param group The name of the group to attach to.
   * @param consumer The name of the consumer to identify as.
   * @param opts The options for the consumer.
   */
  constructor(uri: string, stream: string, group: string, consumer: string, opts: ConsumerOptions)

  /**
   * Creates a new Consumer.
   *
   * @param client An IORedis client instance.
   * @param stream The name of the stream to read from.
   * @param group The name of the group to attach to.
   * @param consumer The name of the consumer to identify as.
   */
  constructor(client: IORedis.Redis, stream: string, group: string, consumer: string)

  /**
   * Creates a new Consumer.
   *
   * @param client An IORedis client instance.
   * @param stream The name of the stream to read from.
   * @param group The name of the group to attach to.
   * @param consumer The name of the consumer to identify as.
   * @param opts The options for the consumer.
   */
  constructor(client: IORedis.Redis, stream: string, group: string, consumer: string, opts: ConsumerOptions)

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

  /**
   * Iterates over the consumer, asynchronously yielding messages
   * in an `[id, data]` pair.
   */
  public [Symbol.asyncIterator]() {
    return this._iterate()
  }

  /**
   * Immediately disconnects from the wrapped Redis client.
   */
  public disconnect() {
    this.client.disconnect()
  }

  /**
   * Allows for graceful shutdown of the consumer, setting up
   * a disconnect to be run after pending messages can be acked.
   */
  public shutdown() {
    this.running = false
    if (this.reading && this.block === 0) {
      this.disconnect()
    }
  }

  /**
   * Attaches an event handler to the wrapped Redis client.
   *
   * @param eventName The name of the event to attach a handler to.
   * @param callback The handler for the event.
   */
  public on(name: string, callback: listener) {
    return this.client.on(name, callback)
  }

  /**
   * Acknowledges successful message delivery by id, removing the message from the pending entries list on the server.
   *
   * @param id The id of the message.
   */
  public async ack(id: Id) {
    await this.client.xack(this.stream, this.group, id)
  }

  /**
   * Returns a list of pending messages for the consumer group.
   *
   * @param start The id to start at.
   * @param end The id to end at.
   * @param length The max length of pending messages to return.
   * @returns A list of pending messages, along with their metadata.
   */
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

  /**
   * Claims pending messages to be re-delievered to this consumer.
   *
   * Noting that even though the claimed messages are returned by the server,
   * they will still be re-delivered during normal iteration.
   *
   * @param ids The ids of the pending messages to claim.
   * @returns A list of `[id, data]` pairs which were claimed.
   */
  public async claim(ids: Id[]): Promise<Array<DeserializedPair<T>>> {
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

  /**
   * Deletes the consumer from the consumer group.
   *
   * Any delievered but unacked messages for this consumer will be discarded from the server.
   */
  public async deleteConsumer() {
    await this.client.xgroup('DELCONSUMER', this.stream, this.group, this.consumer)
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
    try {
      this.reading = true
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
    } catch (err) {
      if (isConnectionClosedError(err) && !this.running) {
        return null
      }
      throw err
    } finally {
      this.reading = false
    }
  }

  private async *_iterate() {
    await this._ensureGroup()
    while (this.running) {
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
    this.disconnect()
  }
}

export default Consumer
export { Consumer, ConsumerOptions }
