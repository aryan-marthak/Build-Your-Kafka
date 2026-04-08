import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn = server.accept() # wait for client
    conn.sendall(b"\x00\x00\x00\x00\x00\x00\x00\x07")


if __name__ == "__main__":
    main()
