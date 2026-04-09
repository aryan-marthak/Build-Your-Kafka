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
