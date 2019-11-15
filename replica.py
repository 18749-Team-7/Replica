import argparse
import hashlib
import json
import socket
import time
import threading
import os
import multiprocessing

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

class Replica():
    """
    Main Man
    """

    def __init__(self, port, verbose):
        self.port = port
        self.set_host_ip()
        self.ip = self.host_ip
        self.verbose = verbose
        self.HB_port = 10000
        self.rp_msg_count = 0
        self.client_msg_queue = multiprocessing.Queue()

        # Client variables

        # Global variables
        self.users = dict()
        self.users_mutex = threading.Lock() # Lock on users dict

        self.msg_count = 0
        self.count_mutex = threading.Lock() # Lock on message_count

        # Start the heartbeat thread
        self.start_heartbeat(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

        threading.Thread(target=self.client_msg_queue_proc).start()

        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.port) + RESET)
        self.chat_server()


    def set_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    ##########################################################
    #   Heartbeat functions
    ##########################################################
    def heartbeat_thread(self, s, interval):
        while(True):
            try:
                packet = '{"type": "heartbeat"}'
                s.send(packet.encode("utf-8"))
                time.sleep(interval)

            except KeyboardInterrupt:
                s.close()
                return

            except Exception as e:
                print(e)
                time.sleep(interval)

    def start_heartbeat(self, interval):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.port) + RESET)

        except Exception as e:
            print(e)
            return

        threading.Thread(target=self.heartbeat_thread,args=(s, interval)).start()

    

    def broadcast(self, message):
        # Inform all other clients that a new client has joined
        self.users_mutex.acquire()
        for _, s_client in self.users.items():
            s_client.send(message.encode("utf-8"))
        self.users_mutex.release()
        return

    def get_hash(self, message, count):
        return hashlib.sha256(message + str(count)).hexdigest()

    def client_service_thread(self, s, addr):
        # Client has connected to the server
        # We expect the first packet from the client to be a JSON login packet {"type": "login", "username":<username>}
        login_data = s.recv(BUF_SIZE)
        login_data = login_data.decode("utf-8")
        login_data = json.loads(login_data)

        if (login_data["type"] != "login"): # Wrong packet type
            # Send error message
            message = dict()
            message["type"] = "error"
            message["text"] = "Malformed packet"
            message = json.dumps(message)
            s.send(message.encode("utf-8"))
            return 

        if (login_data["username"] in self.users): # Username already in use
            # Send failed login packet to new user
            message = dict()
            message["type"] = "error"
            message["text"] = "Username taken"
            message = json.dumps(message)
            s.send(message.encode("utf-8"))
            s.close()
            return     

        # Otherwise, we accept the client
        username = login_data["username"]
        # Add the client socket to the users dictionary
        self.users_mutex.acquire()
        self.users[username] = s
        self.users_mutex.release()


        # Insert job in client queue
        self.client_msg_queue.put(login_data)

        
        # Receive and put message in the queue
        while True:
            try:
                data = s.recv(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)

                self.client_msg_queue.put(data)

                if (data["type"] == "logout"):
                    return

            except:
                print(RED + "Client has disconnected" + RESET)
                return
        return

    def client_msg_queue_proc(self):
        while True:
            # TODO: Insert quiscence control here

            # Get job from the queue and process it
            data = self.client_msg_queue.get()
            username = data["username"]

            # Print received message here
            print(YELLOW + "(RECV) -> {}".format(data) + RESET)
            
            # Login Packet
            if (data["type"] == "login"):
                # Send user joined message to all other users
                message = dict()
                message["type"] = "login_success"
                message["username"] = data["username"]
                message["clock"] = self.rp_msg_count
                message = json.dumps(message)
                self.broadcast(message)
                self.rp_msg_count += 1

            # If the client is attempting to logout
            if (data["type"] == "logout"):
                s = self.users[username]
                # Delete the current client from the dictionary
                self.users_mutex.acquire()
                del self.users[username]
                self.users_mutex.release()

                print("Logout from:", username)

                message = dict()
                message["type"] = "logout_success"
                message["username"] = username
                message["clock"] = self.rp_msg_count
                message = json.dumps(message)
                self.broadcast(message)
                self.rp_msg_count += 1

                s.close()

            # If the client sends a normal chat message
            elif (data["type"] == "send_message"):
                chat_message = data["text"]

                message = dict()
                message["type"] = "receive_message"
                message["username"] = username
                message["text"] = chat_message
                message["clock"] = self.rp_msg_count

                message = json.dumps(message)
                self.broadcast(message)
                self.rp_msg_count += 1

    def chat_server(self):
        # Open listening socket of Replica
        self.s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.ip, self.port))
        self.s.listen(5)

        try:
            while(True):
                # Accept a new connection
                conn, addr = self.s.accept()
                print("Accepted new client")
                # Initiate a client listening thread
                threading.Thread(target=self.client_service_thread, args=(conn, addr)).start()

        except KeyboardInterrupt:
            self.users_mutex.acquire()
            for _, s_client in self.users.items():
                s_client.close()
            self.users_mutex.release()
            self.s.close()
            print(RED + "Closing chat server on " + str(self.ip) + ":" + str(self.port) + RESET)
        except Exception as e:
            print(e)


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--port', help="Server port", type=int, default=5000)
    parser.add_argument('-v', '--verbose', help="Print every chat message", action='store_true')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()

    args = get_args()
    replica_obj = Replica(args.port, args.verbose)

    print("\nTotal time taken: " + str(time.time() - start_time) + " seconds")

    # Exit
    os._exit(1)