import * as IORedis from 'ioredis'

import { serialize } from './serialization'
import { Id, listener } from './types'

type WriterOptions = {
  maxLength?: number
}

const defaultOptions: Required<WriterOptions> = {
  maxLength: 1000,
}

class Writer<T = any> {
  private client: IORedis.Redis
  private maxLength: number

  constructor(uri: string, stream: string)
  constructor(uri: string, stream: string, opts: WriterOptions)
  constructor(client: IORedis.Redis, stream: string)
  constructor(client: IORedis.Redis, stream: string, opts: WriterOptions)
  constructor(clientOpts: IORedis.Redis | string, private stream: string, opts: WriterOptions = defaultOptions) {
    this.client = typeof clientOpts === 'string' ? new IORedis(clientOpts) : clientOpts
    this.maxLength = opts.maxLength || defaultOptions.maxLength
  }

  public async write(data: T): Promise<string> {
    const serialized = serialize(data)
    const id = await this.client.xadd(this.stream, 'MAXLEN', '~', this.maxLength, '*', 'd', serialized)
    return id as Id
  }

  public on(eventName: string, callback: listener): void {
    this.client.on(eventName, callback)
  }

  public disconnect() {
    this.client.disconnect()
  }
}

export default Writer
export { WriterOptions, Writer }
