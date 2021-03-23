"""
Simple reader script that lazily reads a file line by line and forwards it
to the spark streaming master container via a tcp socket.
"""
import threading
import socket
import json


with open("conf.json", "r") as conf_file:
    conf = json.loads(conf_file.read())


def init_sender(conn):
    while True:
        with open(conf["dataset"], "r") as dataset:
            dataset.readline()
            for line in dataset:
                conn.send(line.encode("utf-8"))


def main():
    sock = socket.socket()
    try:
        sock.bind(("", conf["recv-port"]))
        sock.listen()
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=init_sender, args=(conn,)).start()
    except KeyboardInterrupt:
        print("Bye")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
