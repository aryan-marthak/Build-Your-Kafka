import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, _ = server.accept() # wait for client
    data = conn.recv(1024)
    version = int.from_bytes(data[6:8], "big")
    if version <= 4:
        error_code = 0
    else:
        error_code = 35
    error_bytes = error_code.to_bytes(2, "big")
    conn.sendall(b"\x00\x00\x00\x00" + data[8:12] + error_bytes)

if __name__ == "__main__":
    main()
