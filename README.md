# Pub/Sub Event Broker

Built this project mainly to understand Go concurrency better and to get hands-on with worker pools, sharding, retries and channel based communication.

Its a Topic based Pub/Sub broker implemented using a producer-consumer architecture internally.
The broker supports topic based pub/sub, concurrent publishers, retries and a dead letter flow. Internally events are routed to shards using hashing and then processed by worker pools.

## Things in the project

* Topic based subscriptions
* Sharded queues
* Worker pools
* Retry handling
* Dead letter path
* Metrics
* `sync.Pool` usage
* Concurrent publishing

## Running

```bash
go run .
```

## Benchmarks

System used:

```text
AMD Ryzen 5 5500U
Linux amd64
```

Some benchmark numbers from my machine:

```text
BenchmarkNotify                ~384 ns/op
BenchmarkParallelPublish       ~239 ns/op
BenchmarkThroughput            ~714 ns/op
BenchmarkFanout100Subscribers  ~30.7 µs/op
BenchmarkMultipleTopics        ~585 ns/op
BenchmarkQueuePressure         ~285 ns/op
```

To run benchmarks:

```bash
go test ./broker -bench=. -benchmem
```

Race testing:

```bash
go test ./... -race
```

## Small overview

Events are hashed into shards. Every shard has its own queue and worker pool. Failed deliveries are sent to a retry queue and retried until the retry limit is reached.

I also used `sync.Pool` for reusing delivery trackers and reducing allocations a bit.

## Current state

At this point I am stopping work on this version.

Shutdown handling is not finalized because different systems usually want different behaviour. Some may want to drain everything before exit, some may prefer timeout based shutdown and some may just stop immediately.

Dead letter persistence was started but currently disabled.

The core broker flow is there, but further changes depend on what requirements are chosen next.


# PipeLine of Project :


                 +-------------+
                 |  Publisher  |
                 +------+------+
                        |
                        v
                 +-------------+
                 |   Notify()  |
                 +------+------+
                        |
                        v
              +-------------------+
              | Topic Subscribers |
              +-------------------+
                        |
                        v
                Hash Topic (FNV)
                        |
                        v
          +----------------------------+
          | Sharded Queues (CPU Count) |
          +----------------------------+
                 |      |      |
                 v      v      v

          +--------------------------+
          | Worker Goroutines        |
          | (per shard workers)      |
          +------------+-------------+
                       |
                       v
                Subscriber.Update()
                       |
             +---------+---------+
             |                   |
           Success             Failed
             |                   |
             v                   v
      Delivered++          Retry Count++
                                 |
                     +-----------+----------+
                     |                      |
                  Retry < Max         Retry >= Max
                     |                      |
                     v                      v
               Retry Queue           Dead Letter
                     |                      |
                     +----------+-----------+
                                |
                                v
                         Back to Shard
                             Queue
