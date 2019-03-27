import * as IORedis from 'ioredis'

import { serialize } from './serialization'
import { listener } from './types'

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
    const key = await this.client.xadd(this.stream, 'MAXLEN', '~', this.maxLength, '*', 'data', serialized)
    return key as string
  }

  public on(eventName: string, callback: listener): void {
    this.client.on(eventName, callback)
  }
}

export default Writer
export { WriterOptions, Writer }
