import struct
import socket
import threading
import os

LOG_DATA = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"


def encode_compact_size(n):
    """Encode n as an unsigned varint (used for compact bytes/arrays)."""
    out = b""
    while n > 0x7F:
        out += bytes([(n & 0x7F) | 0x80])
        n >>= 7
    out += bytes([n])
    return out


def decode_unsigned_varint(data, offset):
    """Decode an unsigned varint from data at offset. Returns (value, new_offset)."""
    result = 0
    shift = 0
    while True:
        b = data[offset]
        result |= (b & 0x7F) << shift
        offset += 1
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, offset


def decode_signed_varint(data, offset):
    """Decode a zigzag-encoded signed varint. Returns (value, new_offset)."""
    uval, offset = decode_unsigned_varint(data, offset)
    # Zigzag decode
    val = (uval >> 1) ^ -(uval & 1)
    return val, offset


def parse_cluster_metadata():
    """Parse the cluster metadata log and return mappings of topic_id -> topic_name."""
    topic_id_to_name = {}
    topic_name_to_id = {}
    topic_id_partitions = {}  # topic_id -> count of partition records

    try:
        with open(LOG_DATA, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return topic_id_to_name, topic_name_to_id, topic_id_partitions

    pos = 0
    while pos + 12 <= len(data):
        # Record batch header
        base_offset = struct.unpack_from(">q", data, pos)[0]
        batch_length = struct.unpack_from(">i", data, pos + 8)[0]
        
        if batch_length <= 0 or pos + 12 + batch_length > len(data):
            break
        
        batch_end = pos + 12 + batch_length
        
        # Skip rest of batch header:
        # partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
        # last_offset_delta(4) + base_timestamp(8) + max_timestamp(8) +
        # producer_id(8) + producer_epoch(2) + base_sequence(4) +
        # num_records(4)
        header_size = 12 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4
        
        if header_size > len(data):
            break
        
        num_records = struct.unpack_from(">i", data, pos + header_size - 4)[0]
        rec_pos = pos + header_size
        
        for _ in range(num_records):
            if rec_pos >= batch_end:
                break
            
            # Record: length(varint) + attributes(1) + timestamp_delta(varint) +
            #          offset_delta(varint) + key_length(varint) + key + 
            #          value_length(varint) + value + headers_count(varint)
            record_length, rec_pos = decode_signed_varint(data, rec_pos)
            record_start = rec_pos
            
            _attributes = data[rec_pos]
            rec_pos += 1
            
            _timestamp_delta, rec_pos = decode_signed_varint(data, rec_pos)
            _offset_delta, rec_pos = decode_signed_varint(data, rec_pos)
            
            # Key
            key_length, rec_pos = decode_signed_varint(data, rec_pos)
            if key_length > 0:
                rec_pos += key_length
            
            # Value
            value_length, rec_pos = decode_signed_varint(data, rec_pos)
            if value_length > 0:
                value_data = data[rec_pos:rec_pos + value_length]
                rec_pos += value_length
                
                # Parse the value to determine record type
                # Value format: frame_version(1) + type(1) + version(1) + fields...
                if len(value_data) >= 3:
                    frame_version = value_data[0]
                    record_type = value_data[1]
                    record_version = value_data[2]
                    
                    vpos = 3
                    
                    if record_type == 2:  # TopicRecord
                        # Fields: name(compact_string) + topic_id(UUID, 16 bytes)
                        name_len, vpos = decode_unsigned_varint(value_data, vpos)
                        name_len -= 1  # compact string: stored as len+1
                        if name_len > 0 and vpos + name_len + 16 <= len(value_data):
                            topic_name = value_data[vpos:vpos + name_len]
                            vpos += name_len
                            topic_uuid = value_data[vpos:vpos + 16]
                            
                            topic_id_to_name[topic_uuid] = topic_name
                            topic_name_to_id[topic_name] = topic_uuid
                            topic_id_partitions[topic_uuid] = 0
                    
                    elif record_type == 3:  # PartitionRecord
                        # Fields: partition_id(int32) + topic_id(UUID) + ...
                        # partition_id is a zigzag varint in some versions
                        partition_id, vpos = decode_signed_varint(value_data, vpos)
                        if vpos + 16 <= len(value_data):
                            topic_uuid = value_data[vpos:vpos + 16]
                            if topic_uuid in topic_id_partitions:
                                topic_id_partitions[topic_uuid] += 1
            else:
                if value_length == 0:
                    pass  # empty value
            
            # Headers count
            headers_count, _ = decode_unsigned_varint(data, rec_pos)
            
            # Jump to end of record
            rec_pos = record_start + record_length
        
        pos = batch_end
    
    return topic_id_to_name, topic_name_to_id, topic_id_partitions


def read_partition_log(topic_name, partition=0):
    if isinstance(topic_name, bytes):
        topic_name = topic_name.decode("utf-8")
    path = f"/tmp/kraft-combined-logs/{topic_name}-{partition}/00000000000000000000.log"
    print(f"DEBUG: Reading partition log from: {path}", flush=True)
    try:
        with open(path, "rb") as f:
            data = f.read()
            print(f"DEBUG: Read {len(data)} bytes from partition log", flush=True)
            return data
    except FileNotFoundError:
        print(f"DEBUG: File not found: {path}", flush=True)
        return b""


def load_log_data():
    try:
        with open(LOG_DATA, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return b""


def recv_exact(conn, n):
    """Read exactly n bytes from the connection."""
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
        # Read 4-byte message size header
        size_bytes = recv_exact(conn, 4)
        if not size_bytes:
            break
        message_size = int.from_bytes(size_bytes, "big")
        
        # Read exactly message_size bytes
        data = recv_exact(conn, message_size)
        if not data:
            break
        
        # Prepend size bytes so field offsets stay the same as before
        data = size_bytes + data
        
        api_key = int.from_bytes(data[4:6], "big")
        correlation_id = data[8:12]
        
        if api_key == 18:
            version = int.from_bytes(data[6:8], "big")
            if version <= 4:
                error_code = 0
            else:
                error_code = 35
            error_bytes = error_code.to_bytes(2, "big")
            body = (
                error_bytes +
                b"\x04" +
                
                b"\x00\x12" +
                b"\x00\x00" +
                b"\x00\x04" +
                b"\x00" +
                
                b"\x00\x01" +
                b"\x00\x00" +
                b"\x00\x10" +
                b"\x00" +
                
                b"\x00\x4b" +
                b"\x00\x00" +
                b"\x00\x00" +
                b"\x00" +
                
                b"\x00\x00\x00\x00" +
                b"\x00"
            )
            response = correlation_id + body
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
            
        elif api_key == 75:
            # client_id is a nullable string: int16 (signed), -1 means null
            client_id_length = int.from_bytes(data[12:14], "big", signed=True)
            if client_id_length < 0:
                client_id_length = 0
            base = 14 + client_id_length
            base += 1  # skip tag buffer byte
                        
            num_topics = data[base] - 1  # compact array: actual count = byte - 1
            idx = base + 1

            topics_list = []

            for _ in range(num_topics):
                if idx >= len(data):
                    break
                
                topic_len = data[idx] - 1  # compact string: actual len = byte - 1
                idx += 1

                if idx + topic_len > len(data):
                    break
                
                topic_name = data[idx: idx + topic_len]
                idx += topic_len
                idx += 1  # skip per-topic tag buffer

                topics_list.append(topic_name)           
            
            topics_list.sort()
            
            # Parse metadata
            id_to_name, name_to_id, id_partitions = parse_cluster_metadata()
            
            topics_body = b""

            for topic_name in topics_list:
                topic_id = name_to_id.get(topic_name)

                if topic_id is None:
                    error_code = 3
                    topic_id = b"\x00" * 16
                    partitions = b"\x01"
                else:
                    error_code = 0
                    partitions_count = id_partitions.get(topic_id, 1)
                    partitions = bytes([partitions_count + 1])

                    for i in range(partitions_count):
                        partitions += (
                            b"\x00\x00" +
                            i.to_bytes(4, "big") +
                            b"\x00\x00\x00\x01" +
                            b"\x00\x00\x00\x00" +
                            b"\x02" + b"\x00\x00\x00\x01" +
                            b"\x02" + b"\x00\x00\x00\x01" +
                            b"\x01" +
                            b"\x01" +
                            b"\x01" +
                            b"\x00"
                        )

                topics_body += (
                    error_code.to_bytes(2, "big") +
                    bytes([len(topic_name) + 1]) +
                    topic_name +
                    topic_id +
                    b"\x00" +          # is_internal
                    partitions +
                    b"\x00\x00\x00\x00" +  # authorized_operations
                    b"\x00"            # tag buffer
                )
            
            topics_array = bytes([len(topics_list) + 1]) + topics_body
            
            body = (
                b"\x00\x00\x00\x00" +  # throttle_time_ms
                topics_array +
                b"\xff" +              # next_cursor = null
                b"\x00"               # tag buffer
            )
            
            response = correlation_id + b"\x00" + body  # \x00 = response header tag buffer
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
        
        elif api_key == 1:
            client_id_length = int.from_bytes(data[12:14], "big", signed=True)
            if client_id_length < 0:
                client_id_length = 0
            
            base = 14 + client_id_length
            base += 1  # skip tag buffer
            base += (4 + 4 + 4 + 1 + 4 + 4)  # skip fetch-specific fields
            
            num_topics = data[base] - 1  # compact array
            idx = base + 1

            header = correlation_id + b"\x00"

            if num_topics == 0:
                body = (
                    b"\x00\x00\x00\x00" +  # throttle_time_ms
                    b"\x00\x00" +          # error_code
                    b"\x00\x00\x00\x00" +  # session_id
                    b"\x01" +              # responses: compact array length 0 (1 = empty)
                    b"\x00"                # tag buffer
                )
            else:
                topic_id = data[idx: idx + 16]
                idx += 16
                
                # Parse partitions array
                num_partitions = data[idx] - 1  # compact array
                idx += 1
                
                # Parse first partition
                partition_index = int.from_bytes(data[idx: idx + 4], "big")
                idx += 4
                # current_leader_epoch (4 bytes)
                idx += 4
                # fetch_offset (8 bytes)
                fetch_offset = int.from_bytes(data[idx: idx + 8], "big")
                idx += 8
                
                # Parse metadata to find topic name
                id_to_name, name_to_id, id_partitions = parse_cluster_metadata()
                topic_known = topic_id in id_to_name
                
                print(f"DEBUG: topic_id hex: {topic_id.hex()}", flush=True)
                print(f"DEBUG: topic_known: {topic_known}", flush=True)
                
                record_bytes = b""
                
                if not topic_known:
                    partition_error_code = b"\x00\x64"
                    record_bytes = b""
                else:
                    partition_error_code = b"\x00\x00"
                    topic_name = id_to_name[topic_id]
                    print(f"DEBUG: topic_name: {topic_name}", flush=True)
                    record_bytes = read_partition_log(topic_name, partition_index)

                if record_bytes:
                    records_field = encode_compact_size(len(record_bytes) + 1) + record_bytes
                    print(f"DEBUG: record_bytes len: {len(record_bytes)}, records_field len: {len(records_field)}", flush=True)
                else:
                    records_field = b"\x01"  # compact nullable bytes: 1 = empty (0 bytes), 0 = null

                partition = (
                    partition_index.to_bytes(4, "big") +
                    partition_error_code +
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +  # high_watermark
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +  # last_stable_offset
                    b"\x00\x00\x00\x00\x00\x00\x00\x00" +  # log_start_offset
                    b"\x01" +                               # aborted_transactions (empty compact array)
                    b"\xff\xff\xff\xff" +                   # preferred_read_replica = -1
                    records_field +
                    b"\x00"                                 # tag buffer
                )

                topic_block = (
                    topic_id +
                    b"\x02" +   # partitions compact array (1 element)
                    partition +
                    b"\x00"     # tag buffer
                )

                body = (
                    b"\x00\x00\x00\x00" +  # throttle_time_ms
                    b"\x00\x00" +          # error_code
                    b"\x00\x00\x00\x00" +  # session_id
                    b"\x02" +              # responses compact array (1 topic)
                    topic_block +
                    b"\x00"                # tag buffer
                )

            response = header + body
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
        

if __name__ == "__main__":
    main()