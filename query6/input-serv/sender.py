"""
Simple reader script that lazily reads a file line by line and forwards it
to the spark streaming master container via a tcp socket.
"""
import socket
import json


with open("conf.json", "r") as conf_file:
    conf = json.loads(conf_file.read())


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((conf["recv-addr"], conf["recv-port"]))
        with open(conf["dataset"], "r") as dataset:
            dataset.readline()
            for line in dataset:
                sock.send(line.strip())


if __name__ == "__main__":
    main()
