import socket  # noqa: F401
import threading


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
            topic_len = data[-4] - 1
            topic_name = data[-3:]
            
            header = correlation_id + b"\x00"
            
            body = (
                b"\x00\x00\x00\x00" +               # throttle_time_ms
                b"\x02" +                           # 1 topic
                b"\x00\x03" +                       # error_code = 3
                (len(topic_name)+1).to_bytes(1, "big") + topic_name +
                b"\x00"*16 +                        # topic_id
                b"\x00" +                           # is_internal
                b"\x01" +                           # empty partitions
                b"\x00\x00\x00\x00" +               # authorized ops
                b"\x00" +                           # tag buffer
                b"\xff" +                           # next_cursor = -1
                b"\x00"                             # tag buffer
            )

            response = header + body
            size = len(response).to_bytes(4, "big")
            conn.sendall(size + response)

if __name__ == "__main__":
    main()
