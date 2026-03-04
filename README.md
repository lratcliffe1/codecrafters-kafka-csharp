# C# Kafka Broker

This repository contains my solution to the ["Build your own Kafka" challenge](https://app.codecrafters.io/courses/kafka/overview).

Built a Kafka-compatible broker that parses Kafka wire protocol requests over TCP and responds to core APIs including ApiVersions, DescribeTopicPartitions, Fetch, and Produce.

Implemented topic/partition metadata decoding from Kafka's KRaft metadata log, plus on-disk record append/read flow for partition log segments.

## What I have done

Implemented the broker runtime in `src/main.cs` and `src/KafkaServer.cs` that:
listens on TCP port 9092,
accepts concurrent clients asynchronously,
reads framed Kafka messages (`message_size` + request bytes),
parses requests and writes protocol-compliant binary responses.

Added request routing in `src/Helpers/RequestParser.cs` for:
ApiVersions,
DescribeTopicPartitions,
Fetch,
Produce,
unsupported APIs (fallback error response).

Implemented ApiVersions handling in `src/Requests/ApiVersionsRequest.cs` with:
version guard behavior (`UNSUPPORTED_VERSION` for unsupported request versions),
dynamic API key/version range listing based on supported APIs.

Implemented DescribeTopicPartitions handling in `src/Requests/DescribeTopicPartitionsRequest.cs` with:
topic sorting,
unknown-topic error mapping,
per-partition metadata response fields (leader, ISR, replicas, ELR, last-known ELR).

Implemented Fetch handling in `src/Requests/FetchRequest.cs` with:
topic-id and partition parsing from compact request fields,
error handling for unknown topic/partition,
record batch lookup from partition log files and compact nullable bytes responses.

Implemented Produce handling in `src/Requests/ProduceRequest.cs` with:
compact request parsing for topics/partitions/records,
unknown-topic-or-partition handling,
record batch append to partition log files and produce acknowledgements.

Built reusable protocol helpers:
`src/Requests/Base/SharedRequestParsers.cs` for compact/varint/tag-buffer parsing,
`src/Helpers/KafkaResponseWriter.cs` for big-endian encoding, compact types, and response framing.

Added metadata loading/parsing in `src/ClusterMetadata.cs` that:
loads `__cluster_metadata` log on startup,
parses TopicRecord and PartitionRecord entries from record batches,
builds in-memory topic + partition indices for request-time lookups.

## Architecture choices

Used a central request abstraction (`BaseKafkaRequest`) so each API owns its own parse/response logic while the server loop remains transport-focused.

Split protocol concerns into two layers: low-level binary encoding/decoding helpers and high-level request handlers, reducing duplication across API implementations.

Used metadata-first routing by indexing topics both by name and by topic ID to support both DescribeTopicPartitions and Fetch lookup paths efficiently.

Persisted produce records directly to Kafka-style partition log files under `/tmp/kraft-combined-logs` to keep read/write behavior aligned with challenge expectations.

Kept the network loop fully async (`AcceptTcpClientAsync`, `ReadAsync`, `WriteAsync`) to support multiple clients without blocking the listener.

## What I have learnt

Wire-protocol implementations are mostly about disciplined offset management; compact encodings and tagged fields make parser correctness heavily dependent on precise cursor movement.

Separating parsing and response writing utilities early prevents handler code from becoming brittle when adding more Kafka APIs.

A clean request-dispatch boundary makes it easier to support unsupported APIs safely while incrementally adding new handlers.

Kafka metadata logs are rich enough to reconstruct useful broker state (topic IDs, partitions, leaders, ISR) without running a full broker controller.

Binary protocol debugging is much easier when each request type is modeled as a dedicated unit with deterministic serialization output.

## Run locally

Ensure .NET 9 is installed.

Run `./your_program.sh`.
