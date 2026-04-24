# Build Your Own Kafka - Introduction

- This challenge is about building a simplified Kafka broker step by step.
- Kafka is an event streaming system, and this project focuses on the broker side.
- The first thing the project sets up is a TCP server that can talk the Kafka wire protocol.
- This stage is mostly context: it explains that later tasks will involve reading binary requests and sending correctly formatted responses.

## Stage - Multiple Sequential Requests

- The same TCP connection can be reused for more than one Kafka request.
- That means the broker cannot stop after answering the first request.
- The server must keep reading from the socket, detect each full request, and reply once per request.
- Each response still needs the correct `message_size`, the same `correlation_id` from the request, and a valid `ApiVersions` v4 body.
- Because the tester checks the decoded response strictly, there must be no extra bytes left over after the response fields are parsed.

### What this stage teaches

- Kafka is request/response over a persistent connection, not one request per socket.
- Network reads can contain one request, part of a request, or multiple requests, so framing matters.
- Correct byte layout still matters even when the same request repeats many times.

## Stage - Concurrent Requests From Multiple Clients

- Now the broker must handle more than one client at the same time.
- Each client can keep its own TCP connection open and send multiple `ApiVersions` requests.
- The server should accept a new connection without blocking the others, so one slow client does not stop the rest.
- The response rules stay the same for every request: valid `message_size`, matching `correlation_id`, `error_code = 0`, and an `ApiVersions` entry for API key `18` with versions `0` to `4`.

### What this stage teaches

- A Kafka broker is concurrent, not single-client only.
- Each connection must be handled independently.
- The protocol rules do not change just because requests are coming from different clients at the same time.

## Stage - Add DescribeTopicPartitions to ApiVersions

- The ApiVersions response now advertises one more supported API.
- The response still includes the `ApiVersions` entry for API key `18`, and now also includes `DescribeTopicPartitions` with API key `75`.
- This stage does not implement the new API yet; it only tells clients that the broker knows about it.

### Updated `api_keys` array

```text
1) ApiVersions
	api_key = 18
	min_version = 0
	max_version = 4

2) DescribeTopicPartitions
	api_key = 75
	min_version = 0
	max_version = 0
```

- Because the array now has 2 entries, the compact array length becomes `3`.
- Compact arrays always store `actual_count + 1`.

### What this changes in the response

```text
message_size
correlation_id
error_code = 0
api_keys = [18, 75]
throttle_time_ms = 0
tag_buffer = empty
```

### What to remember

- ApiVersions is like the broker's capability list.
- Adding an API here does not mean the API is implemented yet.
- The tester checks that both entries are present and that the version ranges are correct.

## Stage - DescribeTopicPartitions Unknown Topic Response

- In this stage, the broker starts handling API key `75` (`DescribeTopicPartitions`) requests.
- The tester sends one topic name, and the broker must parse that name from the request and echo it in the response.
- For now, every requested topic should be treated as unknown.

### Request fields you need for this stage

- Request header still gives `correlation_id` (must be copied back).
- Request body contains:
	- `topics` as a compact array.
	- each topic contains a compact string `topic_name`.

### Header change in this stage

- Previous stages used response header v0.
- This stage uses response header v1:
	- `correlation_id` (4 bytes)
	- `TAG_BUFFER` (1 byte when empty, value `0x00`)

### Response body requirements (DescribeTopicPartitions v0)

For the single topic in request, return one topic result with:

- `error_code = 3` (`UNKNOWN_TOPIC_OR_PARTITION`)
- `topic_name =` same bytes as in request
- `topic_id =` all-zero UUID (`16` zero bytes)
- `is_internal = false`
- `partitions =` empty compact array
- `topic_authorized_operations = 0`
- `next_cursor = -1` (null, encoded as `0xFF`)

### Compact encoding reminders used here

- Compact array length is stored as `actual_count + 1`.
	- one topic => length byte `0x02`
	- empty partitions => length byte `0x01`
- Compact string length is also unsigned-varint with `actual_length + 1`.
	- topic `foo` (3 bytes) => length byte `0x04`

### Minimal response shape

```text
message_size
response_header_v1:
	correlation_id
	tag_buffer
response_body:
	throttle_time_ms
	topics[1]:
		error_code = 3
		topic_name (echo from request)
		topic_id = 16 zero bytes
		is_internal = false
		partitions = empty
		topic_authorized_operations = 0
		tag_buffer
	next_cursor = -1
	tag_buffer
```

### What this stage teaches

- You now parse a real request body field (`topic_name`) and reflect it in response.
- Header version matters: using v1 means adding header tagged fields byte.
- Kafka compatibility depends on exact binary shape, not just logical values.

## Stage - DescribeTopicPartitions Existing Topic Response

- This stage changes the broker from always saying "unknown topic" to looking up real topic metadata.
- The topic information comes from the cluster metadata log file at `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`.
- The broker must read that log, find the requested topic name, and use the stored UUID and partition data in the response.

### What you need to extract from metadata

- Topic name
- Topic UUID
- Partition IDs for that topic
- Partition leader and replica information

### Response differences for an existing topic

- `error_code = 0`
- `topic_id` is the actual UUID from metadata, not all zeros
- `partitions` contains one partition entry for this stage
- `next_cursor = -1` still means null

### Partition entry shape

For the single partition, the response should include:

- `error_code = 0`
- `partition_index` = the partition ID from metadata
- `leader_id` = broker ID for that partition
- `leader_epoch` = current leader epoch
- `replica_nodes` = broker IDs that host replicas
- `isr_nodes` = in-sync replica broker IDs
- `eligible_leader_replicas` = usually empty here
- `last_known_elr` = usually empty here
- `offline_replicas` = usually empty here
- `topic_authorized_operations = 0`

### What this stage teaches

- Kafka broker metadata is not invented at response time; it comes from the cluster state.
- To answer DescribeTopicPartitions correctly, the broker must parse internal Kafka metadata records.
- The response is now a real metadata lookup, not just a protocol placeholder.

## Stage - DescribeTopicPartitions With Multiple Partitions

- This stage extends the previous one: topic exists, but now it has multiple partitions.
- The response still has one topic entry, but the `partitions` array now contains one full partition object per partition.
- For the test case here, the topic has exactly 2 partitions, so the partitions array must contain exactly 2 entries.

### Core rule for this stage

- Do not merge partition info into one object.
- Build one complete partition block per partition ID.
- Each block has its own `partition_index` and its own `error_code`.

### Response expectations

- Topic-level `error_code = 0`
- Topic name matches request
- Topic UUID matches metadata log
- Partitions array length encodes 2 elements (compact array length byte should be `3`)
- Both partition entries have `error_code = 0`
- `partition_index` values must match real metadata partition IDs
- `next_cursor = -1` (null)

### How your current implementation models this

- It first resolves `topic_id` from the metadata log.
- It estimates partition count by counting occurrences of the topic UUID in log bytes and subtracting one for the topic record itself.
- It writes compact array length as `partitions_count + 1`.
- It loops over partitions and appends one full partition structure per index.

### Quick structure reminder

```text
topics[1]
	topic metadata...
	partitions[2]
		partition #0 entry
		partition #1 entry
	topic_authorized_operations
next_cursor = -1
```

### What this stage teaches

- DescribeTopicPartitions is hierarchical: topic -> partitions.
- Array cardinality and per-element correctness are both validated by the tester.
- Correct protocol encoding now depends on dynamic content size, not fixed static response bytes.
