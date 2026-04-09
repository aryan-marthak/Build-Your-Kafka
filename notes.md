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
