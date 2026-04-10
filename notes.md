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
