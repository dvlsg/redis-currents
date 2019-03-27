type listener = (...args: any[]) => void

type Emitter = {
  on(eventName: string, callback: listener): void
}

export { listener, Emitter }
