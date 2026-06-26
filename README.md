# High Throughput In-Memory Pub/Sub Broker

A concurrent publish/subscribe broker implemented in Go with a focus on throughput, contention reduction and efficient event dispatch.

The broker routes events to sharded queues based on topic hashing. Each shard owns its own subscriber registry and worker pool, allowing publishers to operate concurrently while minimizing shared-state contention.

## Architecture

```text
Publisher
    │
Notify(Event)
    │
Hash Topic
    │
    ▼
Shard
 ├── Copy-on-Write Subscriber Registry
 └── Buffered Queue
          │
          ▼
     Worker Pool
          │
     Batch Processing
          │
          ▼
Subscriber.Update()
```

## Highlights

* Topic-based publish/subscribe
* Sharded event routing
* Copy-on-write subscriber registry (`atomic.Pointer`)
* Worker pool per shard
* Batch processing
* `sync.Pool` for reusable delivery trackers
* Graceful shutdown
* Atomic metrics collection

## Benchmarks

**Environment**

* CPU: AMD Ryzen 5 5500U
* RAM: 8 GB
* OS: Linux

| Metric       |        Result |
| ------------ | ------------: |
| Throughput   | ~2.1M msg/sec |
| Publish Cost |    ~451 ns/op |
| Memory       |       71 B/op |
| Allocations  |    1 alloc/op |

## Next Improvements

* Persistence interface
* P50 / P95 / P99 latency benchmarks
* CPU and memory profiling
* Test and benchmark scripts to fully through test the code adn its latency and througput
