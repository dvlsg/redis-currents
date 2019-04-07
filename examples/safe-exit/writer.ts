// tslint:disable:no-console

import { Writer } from '../../src'
import { sleep } from '../shared'

const run = async () => {
  const { STREAM, WRITER } = process.env
  if (!STREAM) {
    throw new Error(`writer.ts must have STREAM provided from the environment`)
  }
  const uri = 'redis://localhost:6379'

  console.log(`  Connecting writer process to ${uri} for stream ${STREAM}`)

  const writer = new Writer<any>(uri, STREAM)

  let running = true
  process.on('SIGTERM', () => {
    console.log(`  Writer ${WRITER} received SIGTERM, stopping gracefully`)
    running = false
  })

  let i = 0
  while (running) {
    i += 1
    console.log(`  Writer ${WRITER} writing ${i} to stream ${STREAM}`)
    await writer.write({ writer: WRITER, i })
    await sleep(1500)
  }
  await writer.disconnect()
}

run().catch(console.error)
