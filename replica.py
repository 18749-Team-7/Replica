import argparse
import json
import socket
import threading
import os
import time

STR_ENCODING = 'utf-8'

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

local_ip = "127.0.1.1"

def heartbeat_thread(s, interval):
    while(True):
        try:
            packet = '{"function": "heartbeat"}'
            s.send(packet.encode(STR_ENCODING))
            time.sleep(interval)
        except:
            print("Error: Heartbeat failed to send.")

def membership_thread(s):
    while True:
        data = s.recv(BUF_SIZE)
        data = data.decode(STR_ENCODING)
        data = json.loads(data)
        print(json.dumps(data))

        #update our membership set


def replica(port, interval):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
        s.connect((local_ip, port))
        print("Connected to local fault detector at:" + local_ip + ":" + str(port))
    except:
        print("Error: Failed to connect to LFD.")

    threading.Thread(target=heartbeat_thread,args=(s, interval)).start()
    threading.Thread(target=membership_thread,args=(s,)).start()

    try:
        while True:
            pass
            # Do work here later
    except KeyboardInterrupt:
        return


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--port', help="LFD port number", default=10000)
    parser.add_argument('-i', '--interval', help="Heartbeat Interval (sec)", default=1, type=float)
    
    # Parse the arguments
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()

    # Extract Arguments from the 
    args = get_args()

    # Start the Client
    replica(args.port, args.interval)

    # Total client up time
    print(RESET + "\nTime taken: {} seconds".format(time.time() - start_time))

    # Exit
    os._exit(1)