// tslint:disable:no-console

import { ChildProcess, fork, ForkOptions } from 'child_process'
import * as path from 'path'
import { sleep } from '../shared'

const env = {
  STREAM: 'safe-exit-stream',
  GROUP: 'safe-exit-group',
}

const startWriters = async (count = 2) => {
  console.log(`Starting ${count} writer processes`)
  const file = path.join(__dirname, 'writer.ts')
  const args = ['-r', 'ts-node/register']
  let i = 0
  const children: ChildProcess[] = []
  while (++i <= count) {
    const opts: ForkOptions = {
      env: {
        WRITER: String(i),
        ...env,
      },
      silent: false,
    }
    const child = fork(file, args, opts)
    await sleep(500)
    children.push(child)
  }
  return children
}

const startConsumers = (count = 3) => {
  console.log(`Starting ${count} consumer processes`)
  const file = path.join(__dirname, 'consumer.ts')
  const args = ['-r', 'ts-node/register']
  let i = 0
  const children: ChildProcess[] = []
  while (++i <= count) {
    const opts: ForkOptions = {
      env: {
        CONSUMER: `consumer-${i}`,
        ...env,
      },
      silent: false,
    }
    const child = fork(file, args, opts)
    children.push(child)
  }
  return children
}

const run = async () => {
  console.log(`Running safe-exit example`)
  if (process.platform === 'win32') {
    console.warn(`
This example relies on functioning signals to work.
Windows, however, does not support proper signals,
and Node attempts to emulate some signals on Windows,
so this example may not work as expected.

Specifically, SIGTERM is not supported on Windows.

See more here: https://nodejs.org/api/process.html#process_signal_events
`)
    await sleep(3000)
  }

  const writers = await startWriters()
  const consumers = startConsumers()

  console.log(`---- Allowing processes to run for 5 seconds... ----`)
  await sleep(5000)

  console.log(`---- Sending SIGTERM to all consumers... ----`)
  consumers.forEach((consumer, i) => {
    consumer.once('close', (code, signal) => {
      console.log(`Consumer ${i + 1} closed, code: ${code}, signal: ${signal}`)
    })
    console.log(`Sending SIGTERM to consumer ${i + 1}`)
    consumer.kill('SIGTERM')
  })

  console.log(`---- Sending SIGTERM to all writers... ----`)
  writers.forEach((writer, i) => {
    writer.once('close', (code, signal) => {
      console.log(`Writer ${i + 1} closed, code: ${code}, signal: ${signal}`)
    })
    console.log(`Sending SIGTERM to writer ${i + 1}`)
    writer.kill('SIGTERM')
  })
}

run().catch(console.error)
