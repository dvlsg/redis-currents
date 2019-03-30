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
