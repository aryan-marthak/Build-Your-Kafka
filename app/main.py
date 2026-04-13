import socket  # noqa: F401
import threading

LOG_DATA = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

def get_partition_count(topic_name):
    data = load_log_data()
    topic_id = get_topic_id(topic_name)

    if topic_id is None:
        return 0

    partitions = set()
    i = 0

    while True:
        idx = data.find(topic_id, i)
        if idx == -1:
            break

        # read partition index (next 4 bytes)
        if idx + 20 <= len(data):
            p = int.from_bytes(data[idx + 16:idx + 20], "big")

            # only accept small valid numbers
            if p < 10:
                partitions.add(p)

        i = idx + 1

    return len(partitions) if partitions else 1

# def get_partition_count(topic_name):
#     data = load_log_data()
#     topic_id = get_topic_id(topic_name)
    
#     if topic_id is None:
#         return 0
    
#     count = 0
#     i = 0
    
#     while True:
#         idx = data.find(topic_id, i)
#         if idx == -1:
#             break
#         count += 1
#         i = idx + 1
#     return max(1, count // 2)

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
                b"\x03" +              # 2 api (compact array length)
                b"\x00\x12" +          # api_key = 18
                b"\x00\x00" +          # min_version = 0
                b"\x00\x04" +          # max_version = 4
                b"\x00" +              # tag buffer
                b"\x00\x4b" +          # new api_key = 75
                b"\x00\x00" +          
                b"\x00\x00" +
                b"\x00" +              
                b"\x00\x00\x00\x00" +  # throttle_time_ms
                b"\x00"                # tag buffer
            )
            response = correlation_id + body
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
            
        elif api_key == 75:
            
            client_id_length = int.from_bytes(data[12:14], "big")
            base = 14 + max(0, client_id_length) + 1 + 1
            topic_len = data[base] - 1
            topic_name = data[base + 1 : base + 1 + topic_len]
            topic_id = get_topic_id(topic_name)
            if topic_id is None:
                error_code = 3
                topic_id = b"\x00" * 16
                partitions = b"\x01"  # empty partitions
            else:
                error_code = 0
                
                partitions_count = get_partition_count(topic_name)
                partitions = bytes([partitions_count + 1])
                
                for i in range(partitions_count):
                    partitions += (
                        b"\x00\x00" +                 # error_code
                        i.to_bytes(4, "big") +        # partition_index
                        b"\x00\x00\x00\x01" +         # leader_id
                        b"\x00\x00\x00\x00" +         # leader_epoch
                        b"\x02" + b"\x00\x00\x00\x01" +  # replica_nodes
                        b"\x02" + b"\x00\x00\x00\x01" +  # isr_nodes
                        b"\x01" +                     # eligible_leader_replicas
                        b"\x01" +                     # last_known_elr
                        b"\x01" +                     # offline_replicas
                        b"\x00"
                    )
                
            header = correlation_id + b"\x00"
            
            body = (
                b"\x00\x00\x00\x00" +               # throttle_time_ms
                b"\x02" +                           # 1 topic
                error_code.to_bytes(2, "big") +
                bytes([topic_len + 1]) +
                topic_name +
                topic_id +
                b"\x00" +                           # is_internal
                partitions +
                b"\x00\x00\x00\x00" +               # authorized ops
                b"\x00" +                           # tag buffer
                b"\xff" +                           # next_cursor
                b"\x00"
            )
            response = header + body
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)
if __name__ == "__main__":
    main()