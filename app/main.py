import socket
import threading
import struct

LOG_DATA = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"


def encode_compact_size(n):
    out = b""
    while n > 0x7F:
        out += bytes([(n & 0x7F) | 0x80])
        n >>= 7
    out += bytes([n])
    return out


def load_log_data():
    try:
        with open(LOG_DATA, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return b""


def get_topic_id(topic_name):
    if isinstance(topic_name, str):
        topic_name = topic_name.encode()
    data = load_log_data()
    # Search for the TopicRecord signature: \x00\x02\x00 + compact_string(name) + UUID
    pattern = b'\x00\x02\x00' + bytes([len(topic_name) + 1]) + topic_name
    idx = data.find(pattern)
    if idx == -1:
        return None
    uuid_start = idx + len(pattern)
    if uuid_start + 16 > len(data):
        return None
    return data[uuid_start:uuid_start + 16]


def get_topic_name_from_id(topic_id):
    data = load_log_data()
    search_start = 0
    while True:
        idx = data.find(topic_id, search_start)
        if idx == -1:
            return None
        for name_len in range(1, 128):
            name_start = idx - name_len
            varint_pos = name_start - 1
            header_pos = varint_pos - 3
            if header_pos < 0:
                continue
            if (data[varint_pos] == name_len + 1 and
                    data[header_pos + 1] == 0x02 and
                    data[header_pos + 2] == 0x00):
                name = data[name_start:idx]
                if all(32 <= b <= 126 for b in name):
                    return name
        search_start = idx + 1


def get_partition_count(topic_name):
    if isinstance(topic_name, str):
        topic_name = topic_name.encode()
    topic_id = get_topic_id(topic_name)
    if topic_id is None:
        return 0
    
    data = load_log_data()
    # PartitionRecord signature: \x00\x03\x00 + partition_id(4 bytes) + topic_uuid(16 bytes)
    # Count how many PartitionRecords reference this topic_id
    count = 0
    search_start = 0
    while True:
        idx = data.find(topic_id, search_start)
        if idx == -1:
            break
        # Check if this is a PartitionRecord: \x00\x03\x00 + 4-byte partition_id + UUID
        # So UUID is at offset 7 from the \x00\x03\x00 header
        header_pos = idx - 7
        if header_pos >= 0 and data[header_pos:header_pos+3] == b'\x00\x03\x00':
            count += 1
        search_start = idx + 1
    
    return count


def read_partition_log(topic_name, partition=0):
    if isinstance(topic_name, bytes):
        topic_name = topic_name.decode("utf-8")
    path = f"/tmp/kraft-combined-logs/{topic_name}-{partition}/00000000000000000000.log"
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return b""


def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


def handle_client(conn):
    while True:
        size_bytes = recv_exact(conn, 4)
        if not size_bytes:
            break
        message_size = int.from_bytes(size_bytes, "big")
        data = recv_exact(conn, message_size)
        if not data:
            break
        data = size_bytes + data

        api_key = int.from_bytes(data[4:6], "big")
        correlation_id = data[8:12]

        if api_key == 18:  # ApiVersions
            version = int.from_bytes(data[6:8], "big")
            error_code = 0 if version <= 4 else 35
            body = (
                error_code.to_bytes(2, "big") +
                b"\x05" +
                b"\x00\x00" + b"\x00\x00" + b"\x00\x0b" + b"\x00" +  # Produce (0)
                b"\x00\x12" + b"\x00\x00" + b"\x00\x04" + b"\x00" +  # ApiVersions (18)
                b"\x00\x01" + b"\x00\x00" + b"\x00\x10" + b"\x00" +  # Fetch (1)
                b"\x00\x4b" + b"\x00\x00" + b"\x00\x00" + b"\x00" +  # DescribeTopicPartitions (75)
                b"\x00\x00\x00\x00" +
                b"\x00"
            )
            response = correlation_id + body
            conn.sendall(len(response).to_bytes(4, "big") + response)

        elif api_key == 0:  # Produce
            idx = 12
            client_id_len = int.from_bytes(data[idx:idx+2], "big", signed=True)
            idx += 2
            if client_id_len > 0:
                idx += client_id_len
            idx += 1  # tag buffer

            # transactional_id: compact nullable string
            txn_id_len = data[idx] - 1
            idx += 1
            if txn_id_len > 0:
                idx += txn_id_len

            idx += 2  # acks
            idx += 4  # timeout_ms

            num_topics = data[idx] - 1
            idx += 1

            topic_name_len = data[idx] - 1
            idx += 1
            topic_name = data[idx:idx+topic_name_len]
            idx += topic_name_len

            num_partitions = data[idx] - 1
            idx += 1

            partition_index = int.from_bytes(data[idx:idx+4], "big")

            # Validate topic and partition using metadata
            topic_id = get_topic_id(topic_name)

            if topic_id is None:
                # Unknown topic
                error_code = 3
                base_offset = -1
                log_start_offset = -1
            else:
                # Check partition exists
                partition_count = get_partition_count(topic_name)
                if partition_index >= partition_count:
                    error_code = 3
                    base_offset = -1
                    log_start_offset = -1
                else:
                    error_code = 0
                    base_offset = 0
                    log_start_offset = 0

            log_append_time = -1

            partition_response = (
                partition_index.to_bytes(4, "big") +
                error_code.to_bytes(2, "big") +
                base_offset.to_bytes(8, "big", signed=True) +
                log_append_time.to_bytes(8, "big", signed=True) +
                log_start_offset.to_bytes(8, "big", signed=True) +
                b"\x01" +   # record_errors: empty compact array
                b"\x01" +   # error_message: null compact string
                b"\x00"     # tag buffer
            )

            topic_response = (
                bytes([len(topic_name) + 1]) + topic_name +
                b"\x02" +
                partition_response +
                b"\x00"
            )

            body = (
                b"\x02" +
                topic_response +
                b"\x00\x00\x00\x00" +   # throttle_time_ms
                b"\x00"
            )

            response = correlation_id + b"\x00" + body
            conn.sendall(len(response).to_bytes(4, "big") + response)

        elif api_key == 75:  # DescribeTopicPartitions
            client_id_length = int.from_bytes(data[12:14], "big", signed=True)
            if client_id_length < 0:
                client_id_length = 0
            base = 14 + client_id_length + 1  # +1 for tag buffer

            num_topics = data[base] - 1
            idx = base + 1
            topics_list = []

            for _ in range(num_topics):
                if idx >= len(data):
                    break
                topic_len = data[idx] - 1
                idx += 1
                if idx + topic_len > len(data):
                    break
                topic_name = data[idx:idx+topic_len]
                idx += topic_len + 1  # +1 for tag buffer
                topics_list.append(topic_name)

            topics_list.sort()
            topics_body = b""

            for topic_name in topics_list:
                topic_id = get_topic_id(topic_name)
                if topic_id is None:
                    error_code = 3
                    topic_id = b"\x00" * 16
                    partitions = b"\x01"
                else:
                    error_code = 0
                    partitions_count = get_partition_count(topic_name)
                    partitions = bytes([partitions_count + 1])
                    for i in range(partitions_count):
                        partitions += (
                            b"\x00\x00" +
                            i.to_bytes(4, "big") +
                            b"\x00\x00\x00\x01" +
                            b"\x00\x00\x00\x00" +
                            b"\x02" + b"\x00\x00\x00\x01" +
                            b"\x02" + b"\x00\x00\x00\x01" +
                            b"\x01" + b"\x01" + b"\x01" +
                            b"\x00"
                        )
                topics_body += (
                    error_code.to_bytes(2, "big") +
                    bytes([len(topic_name) + 1]) + topic_name +
                    topic_id +
                    b"\x00" +
                    partitions +
                    b"\x00\x00\x00\x00" +
                    b"\x00"
                )

            body = (
                b"\x00\x00\x00\x00" +
                bytes([len(topics_list) + 1]) + topics_body +
                b"\xff" +
                b"\x00"
            )
            response = correlation_id + b"\x00" + body
            conn.sendall(len(response).to_bytes(4, "big") + response)

        elif api_key == 1:  # Fetch
            client_id_length = int.from_bytes(data[12:14], "big", signed=True)
            if client_id_length < 0:
                client_id_length = 0
            base = 14 + client_id_length + 1  # +1 tag buffer
            base += (4 + 4 + 4 + 1 + 4 + 4)  # fetch-specific fields

            num_topics = data[base] - 1
            idx = base + 1
            header = correlation_id + b"\x00"

            if num_topics == 0:
                body = (
                    b"\x00\x00\x00\x00" +
                    b"\x00\x00" +
                    b"\x00\x00\x00\x00" +
                    b"\x01" +
                    b"\x00"
                )
            else:
                topic_id = data[idx:idx+16]
                idx += 16
                num_partitions = data[idx] - 1
                idx += 1
                partition_index = int.from_bytes(data[idx:idx+4], "big")
                idx += 4
                fetch_offset = int.from_bytes(data[idx:idx+8], "big")

                log_data = load_log_data()
                topic_known = topic_id in log_data
                record_bytes = b""

                if not topic_known:
                    partition_error_code = b"\x00\x64"
                else:
                    partition_error_code = b"\x00\x00"
                    topic_name = get_topic_name_from_id(topic_id)
                    if topic_name is not None:
                        record_bytes = read_partition_log(topic_name, partition_index)

                if record_bytes:
                    records_field = encode_compact_size(len(record_bytes) + 1) + record_bytes
                else:
                    records_field = b"\x01"

                partition = (
                    partition_index.to_bytes(4, "big") +
                    partition_error_code +
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +
                    b"\x01" +
                    b"\xff\xff\xff\xff" +
                    records_field +
                    b"\x00"
                )
                topic_block = topic_id + b"\x02" + partition + b"\x00"
                body = (
                    b"\x00\x00\x00\x00" +
                    b"\x00\x00" +
                    b"\x00\x00\x00\x00" +
                    b"\x02" +
                    topic_block +
                    b"\x00"
                )

            response = header + body
            conn.sendall(len(response).to_bytes(4, "big") + response)


if __name__ == "__main__":
    main()