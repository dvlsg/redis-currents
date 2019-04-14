import { assert } from 'chai'
import * as IORedis from 'ioredis'
import * as uuid from 'uuid'

import { Writer } from './Writer'
import { on } from './util'

type Shape = {
  a: number
}

const id = () => uuid.v4()

describe('writer', function() {
  this.timeout(5000)
  const uri = process.env.TEST_REDIS_URI || 'redis://localhost:6379'

  describe('ctor', () => {
    it('should create a writer from a uri', async () => {
      const streamName = id()
      const writer = new Writer<Shape>(uri, streamName)
      assert.instanceOf(writer, Writer)
    })

    it('should create a writer from an ioredis client instance', async () => {
      const client = new IORedis(uri)
      const streamName = id()
      const writer = new Writer<Shape>(client, streamName)
      assert.instanceOf(writer, Writer)
    })
  })

  describe('#on()', () => {
    it('should proxy event handlers to the redis client', async () => {
      const client = new IORedis(uri)
      const streamName = id()
      const writer = new Writer<Shape>(client, streamName)
      await on(writer, 'connect')
    })
  })

  describe('#write()', () => {
    it('should write data to a stream', async () => {
      const client = new IORedis(uri)
      const streamName = id()
      const data = { a: 1 }
      const writer = new Writer<Shape>(client, streamName)
      const wrote = await writer.write(data)
      assert.isString(wrote)
      const state = await client.xinfo('STREAM', streamName)
      const length = state[1]
      assert.strictEqual(length, 1)
    })
  })
})
