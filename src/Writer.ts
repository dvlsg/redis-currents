import * as IORedis from 'ioredis'

import { serialize } from './serialization'
import { Id, listener } from './types'

type WriterOptions = {
  /**
   * The approximate max length for the redis stream.
   *
   * @remarks
   * This value is used by XADD to [cap a stream](https://redis.io/commands/xadd#capped-streams).
   */
  maxLength?: number
}

const defaultOptions: Required<WriterOptions> = {
  maxLength: 1000,
}

/**
 * @param T The type of the data to be written.
 */
class Writer<T = any> {
  private client: IORedis.Redis
  private maxLength: number

  /**
   * Creates a new Writer.
   *
   * @param uri The uri of the redis server.
   * @param stream The name of the stream to write to.
   */
  constructor(uri: string, stream: string)

  /**
   * Creates a new Writer.
   *
   * @param uri The uri of the redis server.
   * @param stream The name of the stream to write to.
   * @param opts The options for the writer.
   */
  constructor(uri: string, stream: string, opts: WriterOptions)

  /**
   * Creates a new Writer.
   *
   * @param client An IORedis client instance.
   * @param stream The name of the stream to write to.
   */
  constructor(client: IORedis.Redis, stream: string)

  /**
   * Creates a new Writer.
   *
   * @param client An IORedis client instance.
   * @param stream The name of the stream to write to.
   * @param opts The options for the writer.
   */
  constructor(client: IORedis.Redis, stream: string, opts: WriterOptions)

  constructor(clientOpts: IORedis.Redis | string, private stream: string, opts: WriterOptions = defaultOptions) {
    this.client = typeof clientOpts === 'string' ? new IORedis(clientOpts) : clientOpts
    this.maxLength = opts.maxLength || defaultOptions.maxLength
  }

  /**
   * Writes a serializable value to the stream.
   *
   * @param data The data to write to the stream.
   * @returns The id for the written data.
   */
  public async write(data: T): Promise<string> {
    const serialized = serialize(data)
    const id = await this.client.xadd(this.stream, 'MAXLEN', '~', this.maxLength, '*', 'd', serialized)
    return id as Id
  }

  /**
   * Attaches an event handler to the wrapped redis client.
   *
   * @param eventName The name of the event to attach a handler to.
   * @param callback The handler for the event.
   */
  public on(eventName: string, callback: listener): void {
    this.client.on(eventName, callback)
  }

  /**
   * Disconnects from redis.
   */
  public disconnect() {
    this.client.disconnect()
  }
}

export default Writer
export { Writer, WriterOptions }
