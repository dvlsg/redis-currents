# redis-currents

Utilities to help direct the flow of [Redis](https://redis.io/) [streams](https://redis.io/topics/streams-intro).

```ts
// Server A
import { Writer } from 'redis-currents'
const writer = new Writer('redis://localhost:6379', 'stream_name')
const write = async () => {
  await writer.write({ data: 1 })
  await writer.write({ data: 2 })
  await writer.write({ data: 3 })
  await writer.write({ data: 4 })
}
write()
```

```ts
// Server B, Process 1
import { Consumer } from 'redis-currents'
const consumer = new Consumer('redis://localhost:6379', 'stream_name', 'group_1', 'consumer_1')
const read = async () => {
  for await (const [id, msg] of consumer) {
    console.log(msg)
    //=> { data: 1 }
    //=> { data: 3 }
    await consumer.ack(id)
  }
}
read()
```

```ts
// Server B, Process 2
import { Consumer } from 'redis-currents'
const consumer = new Consumer('redis://localhost:6379', 'stream_name', 'group_1', 'consumer_2')
const read = async () => {
  for await (const [id, msg] of consumer) {
    console.log(msg)
    //=> { data: 2 }
    //=> { data: 4 }
    await consumer.ack(id)
  }
}
read()
```

```ts
// Server C
import { Consumer } from 'redis-currents'
const consumer = new Consumer('redis://localhost:6379', 'stream_name', 'group_2', 'consumer_1')
const read = async () => {
  for await (const [id, msg] of consumer) {
    console.log(msg)
    //=> { data: 1 }
    //=> { data: 2 }
    //=> { data: 3 }
    //=> { data: 4 }
    await consumer.ack(id)
  }
}
read()
```

## Getting Started

### Requirements

- Redis 5, for access to [streams](https://redis.io/topics/streams-intro).
- Node 10+, for access to [async iterables](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/for-await...of).

### Installation

```sh
yarn add redis-currents
```

### Tests

Tests in `redis-currents` by default will assume that you have an instance of redis running locally on port `6379`.
You can override this by providing `TEST_REDIS_URI` as an environment variable.

```sh
# run all tests
yarn test
```

### Examples

Examples in `redis-currents` can be found at `./examples`, and run from the terminal.

```sh
yarn example:safe-exit
yarn example:groups
```

### API

Docs generated with [TypeDoc](https://typedoc.org/) can be found at https://dvlsg.github.io/redis-currents/
