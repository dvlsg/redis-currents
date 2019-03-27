const serialize = <T>(val: T) => {
  return JSON.stringify(val)
}

const deserialize = <T>(str: string) => {
  return JSON.parse(str) as T
}

export { serialize, deserialize }
