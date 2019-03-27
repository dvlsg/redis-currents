import { Emitter } from './types'

/**
 * Fills an array of integers from `1..n`.
 * @param n The count of elements.
 */
const fill = (n = 1) => [...Array(n).keys()].map(i => i + 1)

/**
 * Returns a promise which resolves the first time a given event is raised.
 * @param emitter The emitter to listen to.
 * @param eventName The event name to listen for.
 */
const on = (emitter: Emitter, eventName: string) =>
  new Promise(resolve => {
    emitter.on(eventName, resolve)
  })

/**
 * Returns a promise which resolves after `ms` milliseconds.
 * @param ms The number of milliseconds in which to resolve.
 */
const sleep = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

export { fill, on, sleep }
