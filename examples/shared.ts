import { fork as childFork, ForkOptions } from 'child_process'

const fork = (file: string, env = {}) => {
  const args = ['-r', 'ts-node/register']
  const opts: ForkOptions = {
    env,
    silent: false,
  }
  const child = childFork(file, args, opts)
  return child
}

const sleep = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

export { fork, sleep }
