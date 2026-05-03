import socket
import threading
import os
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


def decode_varint(data, offset):
    result, shift = 0, 0
    while True:
        b = data[offset]
        result |= (b & 0x7F) << shift
        offset += 1
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, offset


def decode_signed_varint(data, offset):
    uval, offset = decode_varint(data, offset)
    return (uval >> 1) ^ -(uval & 1), offset


def parse_cluster_metadata():
    """Parse cluster metadata log. Returns (name_to_id, id_to_partition_count)."""
    data = load_log_data()
    name_to_id = {}
    id_to_partitions = {}

    pos = 0
    while pos + 12 <= len(data):
        batch_length = struct.unpack_from(">i", data, pos + 8)[0]
        if batch_length <= 0 or pos + 12 + batch_length > len(data):
            break
        batch_end = pos + 12 + batch_length
        # Skip: leader_epoch(4) + magic(1) + crc(4) + attrs(2) + last_offset_delta(4)
        #       + base_ts(8) + max_ts(8) + producer_id(8) + producer_epoch(2) + base_seq(4)
        header_end = pos + 12 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4
        if header_end + 4 > len(data):
            break
        num_records = struct.unpack_from(">i", data, header_end)[0]
        rec_pos = header_end + 4

        for _ in range(num_records):
            if rec_pos >= batch_end:
                break
            record_length, rec_pos = decode_signed_varint(data, rec_pos)
            record_start = rec_pos

            rec_pos += 1  # attributes
            _, rec_pos = decode_signed_varint(data, rec_pos)  # timestamp_delta
            _, rec_pos = decode_signed_varint(data, rec_pos)  # offset_delta

            key_len, rec_pos = decode_signed_varint(data, rec_pos)
            if key_len > 0:
                rec_pos += key_len

            val_len, rec_pos = decode_signed_varint(data, rec_pos)
            if val_len > 0:
                value = data[rec_pos:rec_pos + val_len]
                rec_pos += val_len
                if len(value) >= 3:
                    rec_type = value[1]
                    vpos = 3
                    if rec_type == 2:  # TopicRecord
                        name_varint, vpos = decode_varint(value, vpos)
                        name_len = name_varint - 1
                        if name_len > 0 and vpos + name_len + 16 <= len(value):
                            name = value[vpos:vpos + name_len]
                            uuid = value[vpos + name_len:vpos + name_len + 16]
                            name_to_id[name] = uuid
                            id_to_partitions[uuid] = 0
                    elif rec_type == 3:  # PartitionRecord
                        # partition_id(4) + topic_uuid(16)
                        if vpos + 4 + 16 <= len(value):
                            uuid = value[vpos + 4:vpos + 20]
                            if uuid in id_to_partitions:
                                id_to_partitions[uuid] += 1

            rec_pos = record_start + record_length
        pos = batch_end

    return name_to_id, id_to_partitions


def get_topic_id(topic_name):
    if isinstance(topic_name, str):
        topic_name = topic_name.encode()
    name_to_id, _ = parse_cluster_metadata()
    return name_to_id.get(topic_name)


def get_partition_count(topic_name):
    if isinstance(topic_name, str):
        topic_name = topic_name.encode()
    name_to_id, id_to_partitions = parse_cluster_metadata()
    topic_id = name_to_id.get(topic_name)
    if topic_id is None:
        return 0
    return id_to_partitions.get(topic_id, 0)


def get_topic_name_from_id(topic_id):
    name_to_id, _ = parse_cluster_metadata()
    for name, uid in name_to_id.items():
        if uid == topic_id:
            return name
    return None


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

            # partition_data compact array
            num_partitions = data[idx] - 1
            idx += 1

            partition_index = int.from_bytes(data[idx:idx+4], "big")
            idx += 4

            idx += 4  # current_leader_epoch
        
            # records: compact bytes field (varint of len+1, then raw bytes)
            records_len_varint, idx = decode_varint(data, idx)
            records_len = records_len_varint - 1
            record_batch_bytes = data[idx:idx + records_len] if records_len > 0 else b""

            print(f"DEBUG records_len_varint={records_len_varint} records_len={records_len} batch_len={len(record_batch_bytes)}", flush=True)
            print(f"DEBUG topic={topic_name} topic_id={get_topic_id(topic_name)} partition_count={get_partition_count(topic_name)}", flush=True)

            # Validate using proper metadata parsing
            topic_id = get_topic_id(topic_name)

            if topic_id is None:
                error_code = 3
                base_offset = -1
                log_start_offset = -1
            else:
                partition_count = get_partition_count(topic_name)
                if partition_index >= partition_count:
                    error_code = 3
                    base_offset = -1
                    log_start_offset = -1
                else:
                    error_code = 0
                    base_offset = 0
                    log_start_offset = 0

                    # Write record batch to disk
                    if record_batch_bytes:
                        topic_name_str = topic_name.decode("utf-8") if isinstance(topic_name, bytes) else topic_name
                        log_dir = f"/tmp/kraft-combined-logs/{topic_name_str}-{partition_index}"
                        os.makedirs(log_dir, exist_ok=True)
                        log_path = f"{log_dir}/00000000000000000000.log"
                        with open(log_path, "ab") as f:
                            f.write(record_batch_bytes)

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
            base = 14 + client_id_length + 1

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
                idx += topic_len + 1
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
            base = 14 + client_id_length + 1
            base += (4 + 4 + 4 + 1 + 4 + 4)

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

                topic_name = get_topic_name_from_id(topic_id)
                record_bytes = b""

                if topic_name is None:
                    partition_error_code = b"\x00\x64"
                else:
                    partition_error_code = b"\x00\x00"
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