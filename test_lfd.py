import argparse
import json
import socket
import threading
import os
import time

STR_ENCODING = 'utf-8'

LOGIN_STR = 'login>'
LOGOUT_STR = 'logout>'

BUF_SIZE = 1024

BLACK =     "\u001b[30m"
RED =       "\u001b[31m"
GREEN =     "\u001b[32m"
YELLOW =    "\u001b[33m"
BLUE =      "\u001b[34m"
MAGENTA =   "\u001b[35m"
CYAN =      "\u001b[36m"
WHITE =     "\u001b[37m"
RESET =     "\u001b[0m"
UP =        "\033[A"

TIMEOUT = 5

def heartbeat_thread(s, addr):
    while True:
        data = s.recv(BUF_SIZE)
        data = data.decode(STR_ENCODING)
        data = json.loads(data)
        print(json.dumps(data))



def test_lfd(port):
    try:
        host_ip = socket.gethostbyname(socket.gethostname())
        print(RED + "Starting test LFD on " + str(host_ip) + ":" + str(port) + RESET)

        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.bind((host_ip, port))
        s.listen(5)

        while(True):
            # Accept a new connection
            conn, addr = s.accept()

            # Initiate a client listening thread
            threading.Thread(target=heartbeat_thread, args=(conn, addr)).start()

    except KeyboardInterrupt:
        # Closing the server
        s.close()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--port', help="LFD port", type=int, default=10000)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Extract Arguments from the 
    args = get_args()

    # Start the Client
    test_lfd(args.port)

    # Exit
    os._exit(1)