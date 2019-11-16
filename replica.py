import argparse
import hashlib
import json
import socket
import time
import threading
import os

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

    def __init__(self, verbose):
        self.set_host_ip()
        self.ip = self.host_ip
        self.verbose = verbose
        self.client_port  = 5000
        self.HB_port      = 10000
        self.RM_port      = 15000
        self.replica_port = 20000

        self.good_to_go = False # Set to True once we are up to date with other replicas (via checkpointing + logging)

        # Global variables
        self.users = dict()
        self.users_mutex = threading.Lock() # Lock on users dict

        self.msg_count = 0
        self.count_mutex = threading.Lock() # Lock on message_count

        self.members = dict()
        self.members_mutex = threading.Lock() # Lock on replica members dict

        # Start the heartbeat thread
        self.start_heartbeat(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

        # Start the RM thread
        # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
        # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
        threading.Thread(target=self.rm_thread,args=()).start()


        while (not self.good_to_go):
            pass

        # Start the chat server
        threading.Thread(target=self.print_membership_thread,args=(1,)).start()
        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
        self.chat_server()




    def set_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    ###############################################
    # Heartbeat functions
    ###############################################
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
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            print(e)
            return

        threading.Thread(target=self.heartbeat_thread,args=(s, interval)).start()


    ###############################################
    # Replica Membership functions
    ###############################################
    def print_membership_thread(self, interval):
        while True:
            print("Current Membership Set:" + str(self.members))
            time.sleep(interval)

    def rm_thread(self):
        # Use port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            print(e)


        while True:
            try:
                data,_ = s.recvfrom(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)
                print(data)


                if (data["type"] == "all_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip in self.members:
                            print(RED + "Received add_replicas ip (" + replica_ip + ") that was already in membership set" + RESET)
                        else: 
                            self.members[replica_ip] = None
                    self.members_mutex.release()

                    if self.ip in data["ip_list"]: # First connected as new member, need to get state from other members
                        self.members_mutex.acquire()
                        del self.members[self.ip]
                        self.members_mutex.release()
                        print(RED + "Saw own IP. Getting checkpoint from other replicas" + RESET) 
                        self.get_connection_from_old_replicas()
                    else: # Already member, need to connect to new members
                        self.connect_to_new_replicas()

                elif (data["type"] == "del_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip not in self.members:
                            print(RED + "Received del_replicas ip that was not in membership set" + RESET)
                        else:
                            self.members[replica_ip].close() # close the socket to the failed replica
                            del self.members[replica_ip]
                    self.members_mutex.release()

                else:
                    print(RED + "Received bad packet type from RM" + RESET)


            except KeyboardInterrupt:
                s.close()
                return


    def get_connection_from_old_replicas(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.bind((self.ip, self.replica_port))
        s.listen(5)
        self.members_mutex.acquire()
        try:
            while(s_replica == None for _,s_replica in members.items()):
                # Accept a new connection
                conn, addr = s.accept()
                members[addr] = conn
                print(RED + "Received connection from existing replica at" + addr + RESET)
                threading.Thread(target=self.replica_send_thread(),args=(conn,)).start()
                threading.Thread(target=self.replica_receive_thread(),args=(conn,)).start()
        except Exception as e:
            print(e)
        self.members_mutex.release()
        self.good_to_go = True

    def connect_to_new_replicas(self):
        for addr in members:
            if members[addr] == None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
                    s.connect((addr, self.replica_port))
                    self.members_mutex.acquire()
                    members[addr] = s
                    self.members_mutex.release()
                    print(RED + "Connected to new replica at: " + self.ip + ":" + str(self.replica_port) + RESET)
                    threading.Thread(target=self.replica_send_thread(),args=(s,)).start()
                    threading.Thread(target=self.replica_receive_thread(),args=(s,)).start()

                except Exception as e:
                    print(e)

    def replica_send_thread(self, s):
        replica_to_replica_count = 0
        while True:
            try:
                data = "Ping from " + self.ip + " | " + str(replica_to_replica_count)
                s.send(data.encode("utf-8"))
                replica_to_replica_count = replica_to_replica_count + 1

            except KeyboardInterrupt:
                s.close()
                return
            except Exception as e:
                print(e)
                return

    def replica_receive_thread(self, s):
        while True:
            try:
                data = s.recv(BUF_SIZE)
                data = data.decode("utf-8")
                print(data)
            except KeyboardInterrupt:
                s.close()
                return
            except Exception as e:
                print(e)
                return
    
    ###############################################
    # Chat client functions
    ###############################################
    def broadcast(self, message):
        # send message to all clients
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
            s.close()
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

        print("Login from:", username)

        # Send user joined message to all other users
        message = dict()
        message["type"] = "login_success"
        message["username"] = login_data["username"]
        message = json.dumps(message)
        self.broadcast(message)

        
        # Receive, process, and retransmit chat messages from the client
        while True:
            try:
                data = s.recv(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)

                # If the client is attempting to logout
                if (data["type"] == "logout"):
                    # Delete the current client from the dictionary
                    self.users_mutex.acquire()
                    del self.users[username]
                    self.users_mutex.release()

                    print("Logout from:", username)

                    message = dict()
                    message["type"] = "logout_success"
                    message["username"] = username
                    message = json.dumps(message)
                    self.broadcast(message)

                    s.close()
                    return

                # If the client sends a normal chat message
                elif (data["type"] == "send_message"):
                    chat_message = data["text"]

                    message = dict()
                    message["type"] = "receive_message"
                    message["username"] = username
                    message["text"] = chat_message

                    # users_mutex.acquire()
                    # message["id"] = message_count
                    # message["hash"] = get_hash(chat_message, message_count)
                    # message_count = message_count + 1
                    # users_mutex.release()

                    message = json.dumps(message)
                    self.broadcast(message)
                    
                # Logging
                if(self.verbose): print(message)

            except:
                # Log the user out forcefully
                self.users_mutex.acquire()
                del self.users[username]
                self.users_mutex.release()

                message = dict()
                message["type"] = "logout_success"
                message["username"] = username
                message = json.dumps(message)
                self.broadcast(message)

                s.close()
        return

    def chat_server(self):
        # Open listening socket of Replica
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.bind((self.ip, self.client_port))
        s.listen(5)

        try:
            while(True):
                # Accept a new connection
                conn, addr = s.accept()
                print("Accepted new client")
                # Initiate a client listening thread
                threading.Thread(target=self.client_service_thread, args=(conn, addr)).start()

        except KeyboardInterrupt:
            self.users_mutex.acquire()
            for _, s_client in self.users.items():
                s_client.close()
            self.users_mutex.release()
            s.close()
            print(RED + "Closing chat server on " + str(self.ip) + ":" + str(self.client_port) + RESET)
        except Exception as e:
            print(e)


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-v', '--verbose', help="Print every chat message", action='store_true')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()

    args = get_args()
    replica_obj = Replica(args.verbose)

    print("\nTotal time taken: " + str(time.time() - start_time) + " seconds")

    # Exit
    os._exit(1)