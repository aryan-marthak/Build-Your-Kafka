# Build Your Own Kafka - Introduction

- This challenge is about building a simplified Kafka broker step by step.
- Kafka is an event streaming system, and this project focuses on the broker side.
- The first thing the project sets up is a TCP server that can talk the Kafka wire protocol.
- This stage is mostly context: it explains that later tasks will involve reading binary requests and sending correctly formatted responses.

## Stage 1 - TCP server and hardcoded correlation ID

- Kafka brokers talk to clients over TCP, so the program must listen on port `9092` like a real broker.
- The first response is very small: only 8 bytes total.
- The response format is:

```text
message_size (4 bytes) + correlation_id (4 bytes)
```

- In this stage, the `correlation_id` is hardcoded to `7`.
- The body of the request is ignored for now, because the goal is only to prove the server can accept a connection and send back a valid Kafka-style response.

### Simple picture

```text
client  --->  TCP connection on port 9092  --->  broker
client  <---  00 00 00 00 00 00 00 07   <---  broker response
```

### What to remember

- TCP is the transport layer Kafka uses here.
- The broker does not speak human-readable text.
- Even this tiny response already follows Kafka's binary message layout.

## Stage 2 - Reading the request correlation ID

- Now the broker should stop using a hardcoded correlation ID and read the real one from the request header.
- Kafka messages still follow the same general structure:

```text
message_size + header + body
```

- For this stage, only the header matters.
- The request header version used here is `v2`.
- The important field is `correlation_id`, because the broker must copy it back into the response so the client can match requests and responses.

### Request header layout

```text
request_api_key      (2 bytes)
request_api_version  (2 bytes)
correlation_id       (4 bytes)
client_id            (variable)
tag buffer           (variable)
```

### Example flow

```text
incoming request header
| api_key | api_version | correlation_id | client_id | tags |
			 ^^^^^^^^^^^^^^^
			 read these 4 bytes and reuse them

outgoing response
| message_size | correlation_id |
```

- The broker still does not need to understand the request body.
- The main job is to find the correlation ID at the correct byte offset and echo it back.

### What to remember

- Request headers and response headers are not the same thing.
- Correlation ID is the request tracking number.
- Copying it back is how Kafka keeps request/response pairs aligned.

## Stage 3 - Checking ApiVersions request version

- The broker now starts paying attention to `request_api_version`.
- Kafka APIs are versioned, and each API can support its own range of versions.
- In this stage, the API being handled is `ApiVersions`.

### Why API versions matter

```text
client says: "I am using ApiVersions version X"
broker checks: "Do I support version X?"
```

- If the version is supported, the broker should respond normally.
- If the version is not supported, the broker must return error code `35`, which means `UNSUPPORTED_VERSION`.
- For this challenge stage, the broker can assume it supports `ApiVersions` versions `0` through `4`.

### Response body for this stage

The response now starts to include a real body field:

```text
message_size + correlation_id + error_code
```

- `error_code = 0` means the request version is valid.
- `error_code = 35` means the request version is not supported.

### Tiny illustration

```text
client request
| api_key | api_version | correlation_id |

broker response
| message_size | correlation_id | error_code |
```

### What to remember

- This is the first time the broker uses the request version field.
- APIs have their own version history, so one version number does not apply to every API.
- `ApiVersions` is the broker's way of telling clients what protocol versions it supports.
