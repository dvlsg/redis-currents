// tslint:disable:no-console

import { Writer, Consumer } from '../'

const sleep = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))
const uri = 'redis://localhost:6379'

const write = async (stream: string) => {
  let i = 0
  while (true) {
    const writer = new Writer(uri, stream)
    await writer.write({ i })
    await sleep(500)
    i += 1
  }
}

const consume = async (stream: string, group: string, consumerName: string) => {
  const consumer = new Consumer(uri, stream, group, consumerName)
  for await (const [id, data] of consumer) {
    const str = JSON.stringify(data)
    console.log(`${stream}:${group}:${consumerName} - ${str} @ ${id}`)
    await consumer.ack(id)
  }
}

const run = async () => {
  const stream = 'stream'
  write(stream).catch(console.error)
  consume(stream, 'group1', 'consumer1').catch(console.error)
  consume(stream, 'group1', 'consumer2').catch(console.error)
  consume(stream, 'group2', 'consumer1').catch(console.error)
}

run().catch(console.error)
