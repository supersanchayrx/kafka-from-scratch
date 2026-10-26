# kafka-from-scratch

A Kafka-compatible message broker written from scratch in plain Java, to understand how Kafka turns an append-only file into a replicated, high-throughput log. It speaks the real Kafka wire protocol, so the stock Apache Kafka tools (`kafka-console-producer`, `kafka-console-consumer`, `kafka-producer-perf-test`) connect to it unchanged. Networking is plain `java.nio`, with no frameworks and no runtime dependencies.

> **Status:** work in progress. The design and roadmap are below. Benchmark results will be added here once they are measured.

## Why

Kafka moves millions of messages per second on ordinary disks. It doesn't get there with clever in-memory data structures. It relies on:

- **The log is the storage.** Each partition is an append-only sequence of segment files. Writes are sequential appends, and reads are sequential scans from an offset.
- **The OS does the caching.** Kafka keeps little data on the JVM heap and leans on the page cache. A fetch goes from file to socket with `sendfile` (zero-copy), without passing through user space.
- **Batches end to end.** Producers send record batches. The broker assigns offsets by rewriting the batch header and otherwise stores the bytes as they arrived, and consumers get the same bytes back.
- **Replication by fetching.** Followers pull from the leader the same way consumers do. The leader tracks how far each follower has got, and consumers only see messages that every in-sync replica has (the high watermark).

This project rebuilds each of those pieces in Java, then measures how close it gets to the real thing.

## Design

| Component | What it does |
|---|---|
| **Network layer** | NIO selector threads read size-prefixed requests (a 4-byte length, then API key, version, correlation id and client id), dispatch them by API key, and write responses back in request order on each connection |
| **Protocol** | Hand-written encoders and decoders for the request and response versions the stock tools use, including flexible versions (compact strings and arrays, tagged fields) |
| **APIs** | `ApiVersions` `Metadata` `CreateTopics` `Produce` `Fetch` `ListOffsets` `InitProducerId` `FindCoordinator` `JoinGroup` `SyncGroup` `Heartbeat` `LeaveGroup` `OffsetCommit` `OffsetFetch` |
| **Log** | One directory per partition, holding segments: a `.log` file of record batches (format v2, CRC-32C checked on append) and a sparse `.index` from offset to file position. Segments roll by size, and old ones are deleted by retention time or size |
| **Fetch path** | Finds the segment and file position through the index, then sends the bytes with `FileChannel.transferTo` (zero-copy) |
| **Recovery** | On restart, rescans the active segment, validates each batch's CRC and truncates a torn tail |
| **Idempotent producer** | `InitProducerId` hands out producer ids; per-partition sequence numbers reject duplicates and gaps, so producer retries don't write a message twice |
| **Replication** | A static 3-broker cluster from a config file. Followers fetch from the leader; the leader tracks each replica's log end offset, maintains the in-sync replica set (ISR) and advances the high watermark. `acks=all` produces are answered once the ISR has the batch |
| **Leader failover** | A simple controller (the lowest live broker id) promotes an in-sync follower when a leader stops responding, and bumps the leader epoch so the old leader's late writes are fenced off |
| **Consumer groups** | A group coordinator runs the classic join/sync rebalance (the group's leader client computes the assignment), expires members that stop heartbeating, and stores committed offsets in an internal log that is replayed on startup |

## Run (planned)

Requires JDK 21 and Maven.

```bash
mvn package
java -jar target/kafka-from-scratch.jar --broker-id 0 --port 9092 --log-dir data/0
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning
```

## Benchmarks (planned)

Using the stock perf tools against this broker and against real Apache Kafka on the same machine:

- producer throughput (records/s and MB/s) with `kafka-producer-perf-test`, at 100 B and 1 KB records, `acks=1` vs `acks=all`;
- p50 / p99 produce latency;
- consumer throughput with `kafka-consumer-perf-test`, and zero-copy fetch vs copying through the heap;
- failover: `kill -9` the leader during an `acks=all` run, then count acknowledged messages that are missing.

The method and raw results will live in `BENCHMARKS.md`.

## Roadmap

- [x] Scope and design
- [ ] Network layer and request framing; `ApiVersions` and `Metadata`
- [ ] Single-partition log: append record batches, CRC check, offset index
- [ ] `Produce` and `Fetch`: console producer to console consumer round trip
- [ ] Segment rolling, retention and torn-tail recovery
- [ ] Zero-copy fetch with `transferTo`
- [ ] Topics with many partitions: `CreateTopics`, `ListOffsets`
- [ ] Idempotent producer: `InitProducerId` and sequence checks
- [ ] Consumer groups: coordinator, join/sync/heartbeat, offset commits
- [ ] 3-broker replication: follower fetch, ISR, high watermark, `acks=all`
- [ ] Leader failover with leader epochs
- [ ] Tests: split frames, log recovery, replication under failure
- [ ] Benchmarks against real Kafka

## Planned structure

```
src/main/java/.../
  network/    # selector threads, request framing
  protocol/   # request and response schemas, flexible-version encoding
  api/        # one handler per API key
  log/        # segments, offset index, record batches, recovery
  replica/    # follower fetcher, ISR, high watermark
  group/      # group coordinator, offset store
  cluster/    # static config, metadata, controller
```

## Not planned

Transactions and exactly-once, KRaft (the controller here is deliberately simple), log compaction, TLS and SASL, quotas, ACLs and tiered storage.

## References

- Kafka protocol guide: https://kafka.apache.org/protocol
- Kafka design documentation: https://kafka.apache.org/documentation/#design
- Jay Kreps, *The Log: What every software engineer should know about real-time data's unifying abstraction* (2013): https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
