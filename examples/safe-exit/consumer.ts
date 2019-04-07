// tslint:disable:no-console

import { Consumer } from '../../src'
import { sleep } from '../shared'

const run = async () => {
  const { STREAM, GROUP, CONSUMER } = process.env
  if (!STREAM) {
    throw new Error(`consumer.ts must have STREAM provided from the environment`)
  }
  if (!GROUP) {
    throw new Error(`consumer.ts must have GROUP provided from the environment`)
  }
  if (!CONSUMER) {
    throw new Error(`consumer.ts must have CONSUMER provided from the environment`)
  }

  const uri = 'redis://localhost:6379'
  const opts = { from: '0', block: 0 }
  const consumer = new Consumer<any>(uri, STREAM, GROUP, CONSUMER, opts)

  console.log(`  Connecting consumer process to ${uri} for stream ${STREAM}, group ${GROUP}, consumer ${CONSUMER}`)

  process.on('SIGTERM', () => {
    console.log(`  Consumer ${CONSUMER} received SIGTERM, calling Consumer#shutdown()`)
    consumer.shutdown()
  })

  for await (const [id, data] of consumer) {
    const str = JSON.stringify(data)
    console.log(`  Consumer ${CONSUMER} received id: ${id}, data: ${str}, processing...`)
    await sleep(500)
    console.log(`  Consumer ${CONSUMER} successfully processed id: ${id}`)
    await consumer.ack(id)
  }
}

run().catch(console.error)
