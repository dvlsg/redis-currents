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

const step = <T>(consumer: Consumer<T>) =>
  consumer[Symbol.asyncIterator]()
    .next()
    .then(({ value }) => value)

describe('Consumer', function() {
  this.timeout(5000)

  const uri = process.env.TEST_REDIS_URI || 'redis://localhost:6379'
  const redis = new IORedis(uri)

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

  describe('#pending()', () => {
    it('should have an empty response when calling pending on a group that does not exist yet', async () => {
      const { consumer } = open()
      const pending = await consumer.pending()
      assert.deepEqual(pending, [])
    })

    it('should have an empty response when no unacked messages are pending', async () => {
      const { writer, consumer } = open()
      await writer.write({ a: 1 })
      const [id] = await step(consumer)
      await consumer.ack(id)
      const pending = await consumer.pending()
      assert.deepEqual(pending, [])
    })

    it('should not consider written messages to be pending unless they have been delivered to a consumer', async () => {
      const { writer, consumer } = open()
      await writer.write({ a: 1 })
      const pending = await consumer.pending()
      assert.deepEqual(pending, [])
    })

    it('should return a list of pending but unacked messages', async () => {
      const stream = uuid()
      const group = uuid()
      const consumerName1 = uuid()
      const consumerName2 = uuid()
      const count = 3
      const { writer, consumer: consumer1 } = open({ stream, group, count, consumer: consumerName1 })
      const { consumer: consumer2 } = open({ stream, group, count, consumer: consumerName2 })
      const { consumer: consumer2Again } = open({ stream, group, count, consumer: consumerName2 })
      for (const a of fill(5)) {
        await writer.write({ a })
      }
      await step(consumer1)
      await step(consumer2)
      await step(consumer2Again)
      const pending = await consumer1.pending()
      assert.containSubset(pending, [
        ...fill(3).map(_ => ({
          consumer: consumerName1,
          deliveries: 1,
          group,
          stream,
        })),
        ...fill(2).map(_ => ({
          consumer: consumerName2,
          deliveries: 2,
          group,
          stream,
        })),
      ])
      for (const entry of pending) {
        assert.isString(entry.id)
        assert.instanceOf(entry.created, Date)
        assert.isAtLeast(entry.waiting, 0)
      }
    })
  })

  describe('#claim()', () => {
    it('should claim a list of unacked messages by their ids', async () => {
      const stream = uuid()
      const group = uuid()
      const consumerName1 = uuid()
      const consumerName2 = uuid()
      const { writer, consumer: consumer1 } = open({ stream, group, consumer: consumerName1 })
      const { consumer: consumer2 } = open({ stream, group, consumer: consumerName2 })
      for (const a of fill(5)) {
        await writer.write({ a })
      }
      await step(consumer1)
      const pendingBefore = await consumer2.pending()
      assert.containSubset(pendingBefore, [
        {
          consumer: consumerName1,
          deliveries: 1,
        },
      ])
      const ids = pendingBefore.filter(p => p.consumer !== consumerName2).map(p => p.id)
      const claimed = await consumer2.claim(ids)
      const pendingAfter = await consumer2.pending()
      assert.containSubset(pendingAfter, [
        {
          consumer: consumerName2,
          deliveries: 1,
        },
      ])
      assert.deepEqual(pendingBefore[0].created, pendingAfter[0].created)
      const value = await step(consumer2)
      assert.deepEqual(claimed[0], value)
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

      const pending = await consumer1.pending()
      const pendingForConsumer = pending.filter(p => p.consumer === opts.consumer)
      const unacked = pendingForConsumer.length

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

  describe('#ack()', () => {
    it('should acknowledge a delivered id', async () => {
      const { writer, consumer } = open()
      await writer.write({ a: 1 })
      await writer.write({ a: 2 })
      const [id] = await step(consumer)
      const pendingBefore = await consumer.pending()
      assert.containSubset(pendingBefore, [{ id }])
      await consumer.ack(id)
      const pendingAfter = await consumer.pending()
      assert.deepEqual(pendingAfter, [])
    })

    it('should not error when called before stream or consumer group exists', async () => {
      const { consumer } = open()
      await consumer.ack('invalid-id')
    })

    it('should throw an error when given an invalid id on an initialized stream / group', async () => {
      const { writer, consumer } = open()
      await writer.write({ a: 1 })
      await step(consumer)
      const error = await consumer.ack('invalid-id').catch(e => e)
      assert.instanceOf(error, Error)
      assert.match(error.message, /invalid stream id/gi)
    })
  })

  describe('#shutdown()', () => {
    it('should break the consumer loop even on infinite blocks', async () => {
      const { writer, consumer } = open({ block: 0 })
      await writer.write({ a: 1 })
      for await (const [id, data] of consumer) {
        assert.notStrictEqual(data.a, 2)
        await consumer.ack(id)
        await writer.write({ a: 2 })
        consumer.shutdown()
      }
    })

    it('should continue consuming a delivered block when using a delivery count', async () => {
      const { writer, consumer } = open({ block: 0, count: 3 })
      await writer.write({ a: 1 })
      await writer.write({ a: 2 })
      await writer.write({ a: 3 })
      const consumed: Shape[] = []
      for await (const [id, data] of consumer) {
        if (data.a === 1) {
          consumer.shutdown()
        }
        consumed.push(data)
        await consumer.ack(id)
      }
      assert.deepEqual(consumed, [{ a: 1 }, { a: 2 }, { a: 3 }])
    })
  })

  describe('#deleteConsumer()', () => {
    const checkInfo = async (stream: string, expectedGroup: string, expectedConsumerCount: number) => {
      let info
      try {
        info = await redis.xinfo('GROUPS', stream)
      } catch (err) {
        if (err.message !== 'ERR no such key' || expectedConsumerCount !== 0) {
          throw err
        }
      }
      if (info && info.length > 0) {
        assert.strictEqual(info[0][1], expectedGroup)
        assert.strictEqual(info[0][3], expectedConsumerCount)
      } else {
        assert.strictEqual(0, expectedConsumerCount)
      }
    }

    it('should remove the reference to the consumer from the group', async () => {
      const stream = uuid()
      const group = uuid()
      const { writer, consumer } = open({ stream, group, block: 0 })
      await checkInfo(stream, group, 0)
      await writer.write({ a: 1 })
      await checkInfo(stream, group, 0)
      await step(consumer)
      await checkInfo(stream, group, 1)
      await consumer.deleteConsumer()
      await checkInfo(stream, group, 0)
    })

    it('should only remove the reference of the current consumer from the group', async () => {
      const stream = uuid()
      const group = uuid()
      const { writer, consumer: consumer1 } = open({ stream, group, block: 0 })
      const { consumer: consumer2 } = open({ stream, group, block: 0 })
      await checkInfo(stream, group, 0)
      await writer.write({ a: 1 })
      await writer.write({ a: 2 })
      await checkInfo(stream, group, 0)
      await step(consumer1)
      await checkInfo(stream, group, 1)
      await step(consumer2)
      await checkInfo(stream, group, 2)
      await consumer1.deleteConsumer()
      await checkInfo(stream, group, 1)
      await consumer2.deleteConsumer()
      await checkInfo(stream, group, 0)
    })

    it('will remove unacked / unclaimed messages when deleting a consumer', async () => {
      const stream = uuid()
      const group = uuid()
      const consumerName1 = uuid()
      const consumerName2 = uuid()
      const { writer, consumer: consumer1 } = open({ stream, group, block: 0, consumer: consumerName1 })
      const { consumer: consumer2 } = open({ stream, group, block: 0, consumer: consumerName2 })
      await writer.write({ a: 1 })
      await writer.write({ a: 2 })
      await step(consumer1)
      const pendingBefore = await consumer2.pending()
      assert.lengthOf(pendingBefore, 1)
      assert.containSubset(pendingBefore[0], { stream, group, consumer: consumerName1, deliveries: 1 })
      await consumer1.deleteConsumer()
      const pendingAfter = await consumer2.pending()
      assert.lengthOf(pendingAfter, 0)
    })
  })
})
