type listener = (...args: any[]) => void

type Emitter = {
  on(eventName: string, callback: listener): void
}

type Id = string

export { listener, Emitter, Id }
