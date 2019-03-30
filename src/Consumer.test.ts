import { assert } from 'chai'
import * as uuid from 'uuid/v4'
import * as IORedis from 'ioredis'

import Consumer from './Consumer'
import Writer from './Writer'

import { on, sleep, fill } from './util'

type OpenOptions = {
  stream?: string
  group?: string
  consumer?: string
  client?: IORedis.Redis
  block?: number
  count?: number
  from?: string
}

type Shape = {
  a: number
}

describe('Consumer', function() {
  this.timeout(5000)
  const uri = 'redis://localhost:6379'
  const redis = new IORedis(uri)
  const flush = redis.flushall.bind(redis)

  afterEach(flush)

  const open = (
    opts: OpenOptions = {
      stream: uuid(),
      group: uuid(),
      consumer: uuid(),
    },
  ) => {
    const { block, from, count, stream = uuid(), consumer: consumerName = uuid(), group = uuid() } = opts
    const consumerOpts = { block, from, count }
    const consumer = new Consumer<Shape>(opts.client || new IORedis(uri), stream, group, consumerName, consumerOpts)
    const writer = new Writer<Shape>(opts.client || new IORedis(uri), stream)
    return { consumer, writer }
  }

  describe('ctor', () => {
    it('should create a consumer from a uri', async () => {
      const streamName = uuid()
      const consumerName = uuid()
      const groupName = uuid()
      const consumer = new Consumer<Shape>(uri, streamName, groupName, consumerName)
      assert.instanceOf(consumer, Consumer)
    })

    it('should create a consumer from an ioredis client instance', async () => {
      const client = new IORedis()
      const streamName = uuid()
      const consumerName = uuid()
      const groupName = uuid()
      const consumer = new Consumer<Shape>(client, streamName, groupName, consumerName)
      assert.instanceOf(consumer, Consumer)
    })
  })

  describe('#on()', () => {
    it('should proxy event handlers to the redis client', async () => {
      const client = new IORedis()
      const streamName = uuid()
      const consumerName = uuid()
      const groupName = uuid()
      const consumer = new Consumer<Shape>(client, streamName, groupName, consumerName)
      await on(consumer, 'connect')
    })
  })

  describe('[Symbol.asyncIterator]()', () => {
    it('should allow us to treat the Consumer as directly iterable', async () => {
      const { consumer, writer } = open()

      const writes = 3

      let writing = 0
      while (++writing <= writes) {
        await writer.write({ a: writing })
      }

      let reading = 0
      const consumed: Shape[] = []
      for await (const [id, data] of consumer) {
        consumed.push(data)
        await consumer.ack(id)
        if (++reading === writes) {
          break
        }
      }
      const expected = [{ a: 1 }, { a: 2 }, { a: 3 }]
      assert.deepEqual(consumed, expected)
    })

    it('should allow consumers to await stream iteration before any writes occur', async () => {
      const stream = uuid()
      const { consumer } = open({ stream })
      const consumed: Shape[] = []
      ;(async () => {
        for await (const [id, data] of consumer) {
          consumed.push(data)
          await consumer.ack(id)
        }
      })()
      await sleep(100)
      await redis.xinfo('STREAM', stream)
    })

    it('should accept a starting id for consumers', async () => {
      const stream = uuid()

      const { writer } = open({ stream })
      const offset = 5
      for (const writing of fill(offset)) {
        await writer.write({ a: writing })
      }
      const state = await redis.xinfo('STREAM', stream)
      const lastId = state[9] as string
      const { consumer } = open({ stream, from: lastId })

      const consumed: Shape[] = []
      const writes = 15
      let reading = 0
      const reads = (async () => {
        for await (const [id, data] of consumer) {
          consumed.push(data)
          await consumer.ack(id)
          if (++reading === writes) {
            break
          }
        }
      })()
      for (const writing of fill(writes).map(i => i + offset)) {
        await writer.write({ a: writing })
      }
      await reads

      const expected = fill(writes).map(i => ({ a: i + offset }))
      assert.deepEqual(consumed, expected)
    })

    it('should allow us to iterate multiple times over the same consumer', async () => {
      const { consumer, writer } = open()
      const writes = 20

      const consumed: Shape[] = []
      const reader = () =>
        (async () => {
          for await (const [id, data] of consumer) {
            consumed.push(data)
            await consumer.ack(id)
          }
        })()

      fill(5).map(reader)
      for (const writing of fill(writes)) {
        await writer.write({ a: writing })
      }

      await sleep(50)
      const expected = fill(20).map(a => ({ a }))
      assert.deepEqual(consumed, expected)
    })

    it('should redeliver unacked items for a newly connected consumer with the same group name', async () => {
      const stream = uuid()
      const consumer = uuid()
      const group = uuid()
      const count = 5
      const opts = { stream, consumer, count, group }

      const { consumer: consumer1, writer } = open(opts)
      const writes = 5
      for (const writing of fill(writes)) {
        await writer.write({ a: writing })
      }

      const consumed: Shape[] = []
      let consumedCount = 0
      for await (const [id, data] of consumer1) {
        consumed.push(data)
        consumedCount += 1
        if (consumedCount === 2) {
          break
        }
        await consumer1.ack(id)
      }

      const pending = await redis.xpending(stream, group)
      const consumers = pending[3]
      const unacked = Number(consumers.find((c: any) => c[0] === consumer)[1])

      assert.strictEqual(consumed.length, 2)
      assert.strictEqual(unacked, 4)

      const { consumer: consumer2 } = open(opts)
      for await (const [id, data] of consumer2) {
        consumed.push(data)
        consumedCount += 1
        await consumer2.ack(id)
        if (consumedCount === 6) {
          break
        }
      }
      const expected = [1, 2, 2, 3, 4, 5].map(i => ({ a: i }))
      assert.deepEqual(consumed, expected)
    })
  })
})
