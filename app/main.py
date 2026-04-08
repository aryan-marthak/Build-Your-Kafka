import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, _ = server.accept() # wait for client
    data = conn.recv(1024)
    conn.sendall(b"\x00\x00\x00\x00" + data[8:12])
    


if __name__ == "__main__":
    main()
