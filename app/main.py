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
        
        version = int.from_bytes(data[6:8], "big")
        if version <= 4:
            error_code = 0
        else:
            error_code = 35
        error_bytes = error_code.to_bytes(2, "big")

        correlation_id = data[8:12]

        body = (
            error_bytes +
            b"\x02" +              # 1 api (compact array length)
            b"\x00\x12" +          # api_key = 18
            b"\x00\x00" +          # min_version = 0
            b"\x00\x04" +          # max_version = 4
            b"\x00" +              # tag buffer
            b"\x00\x00\x00\x00" +  # throttle_time_ms
            b"\x00"                # tag buffer
        )

        response = correlation_id + body
        size = len(response).to_bytes(4, "big")

        conn.sendall(size + response)

if __name__ == "__main__":
    main()
