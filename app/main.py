from email.mime import base
from pydoc_data.topics import topics
import socket  # noqa: F401
import threading

LOG_DATA = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

def get_partition_count(topic_name):
    data = load_log_data()
    topic_id = get_topic_id(topic_name)
 
    if topic_id is None:
        return 0
 
    # The topic UUID appears once in the topic record, then once per
    # partition record. So total occurrences - 1 = partition count.
    count = 0
    i = 0
    while True:
        idx = data.find(topic_id, i)
        if idx == -1:
            break
        count += 1
        i = idx + 1
 
    return max(1, count - 1)

def load_log_data():
    try:
        with open(LOG_DATA, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return b""

def get_topic_id(topic_name):
    data = load_log_data()
    idx = data.find(topic_name)
    if idx == -1:
        return None
    return data[idx + len(topic_name) : idx + len(topic_name) + 16]

def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

def handle_client(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        
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

            topics = []

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

                topics.append(topic_name)           
            
            topics.sort()
            
            topics_body = b""

            for topic_name in topics:
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
            
            topics_array = bytes([len(topics) + 1]) + topics_body
            
            body = (
                b"\x00\x00\x00\x00" +  # throttle_time_ms
                topics_array +
                b"\xff" +              # next_cursor = null
                b"\x00"               # tag buffer
            )
            
            response = correlation_id + b"\x00" + body  # \x00 = response header tag buffer
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
if __name__ == "__main__":
    main()