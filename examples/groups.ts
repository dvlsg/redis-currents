// tslint:disable:no-console

import { Writer, Consumer } from '../'
import { sleep } from './shared'

const uri = 'redis://localhost:6379'

const write = async (stream: string) => {
  console.log(`Initiating writer for stream "${stream}"`)
  let i = 0
  while (true) {
    const writer = new Writer(uri, stream)
    console.log(`Writing { ${i} } to stream "${stream}"`)
    await writer.write({ i })
    await sleep(2000)
    i += 1
  }
}

const consume = async (stream: string, group: string, consumerName: string) => {
  console.log(`Attaching to stream "${stream}", group "${group}", as consumer "${consumerName}"`)
  const consumer = new Consumer(uri, stream, group, consumerName)
  for await (const [id, data] of consumer) {
    const str = JSON.stringify(data)
    console.log(`Consumer received ${stream}:${group}:${consumerName} - ${str} @ ${id}`)
    await consumer.ack(id)
  }
}

const run = async () => {
  console.log(`
This example will demonstrate two consumer groups -
one which will spread writes across two consumers,
and one which will receive all writes in a single consumer.

Press Ctrl+C to exit.
`)
  await sleep(3000)
  const stream = 'stream'
  write(stream).catch(console.error)
  consume(stream, 'group1', 'consumer1').catch(console.error)
  consume(stream, 'group1', 'consumer2').catch(console.error)
  consume(stream, 'group2', 'consumer1').catch(console.error)
}

run().catch(console.error)
