import argparse
import hashlib
import json
import socket
import time
import threading
import os
import multiprocessing
import sys

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

    def __init__(self, verbose=True):
        self.set_host_ip()
        self.ip = self.host_ip
        self.verbose = verbose
        self.client_port  = 5000
        self.HB_port      = 10000
        self.RM_port      = 15000
        self.replica_port = 20000

        # Queues and Dicts
        self.main_msg_count = 0
        self.client_msg_queue = multiprocessing.Queue()
        self.manager = multiprocessing.Manager()
        self.client_msg_dict = self.manager.dict()
        self.per_client_msg_count = {}
        

        # Flag to indicate if checkpoint was done
        self.ckpt_received = False

        self.good_to_go = False # Set to True once we are up to date with other replicas (via checkpointing + logging)

        # Global variables
        self.users = dict()
        self.users_mutex = threading.Lock() # Lock on users dict

        self.msg_count = 0
        self.count_mutex = threading.Lock() # Lock on message_count

        self.members = dict()
        self.members_mutex = threading.Lock() # Lock on replica members dict

        # Passive replication fields
        self.is_primary = False
        self.checkpoint_interval = None
        self.quiesce_lock = threading.Lock() # Lock for active replicas when creating and sending checkpoint
        self.is_in_quiescence = False # Flag to allow printing of logs during quiescence
        self.checkpoint_lock = threading.Lock() # Lock for passive replicas when receiving and implementing checkpoint
        self.log_file_name = "log.txt"
        self.size_of_log = 0

        # self.primary_ip = None # This field should be unnecessary. We can listen to all other replicas all the time and make the assumption that only the 
        # actual primary will send the checkpoint. 

        # Start the heartbeat thread
        self.start_heartbeat(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

        # Start the RM thread
        # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
        # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
        # It will also start checkpoint_receive_threads for each other replica
        threading.Thread(target=self.rm_thread, daemon=True).start()

        # Start the checkpoint_send_thread
        threading.Thread(target=self.checkpoint_send_thread, daemon=True).start()

        # Start the chat server
        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
        threading.Thread(target=self.client_message_processing_thread, daemon=True).start()
        self.chat_server()


    def set_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            print(e)
            return

        threading.Thread(target=self.heartbeat_thread,args=(s, interval), daemon=True).start()


    ###############################################
    # Replica Membership functions
    ###############################################
    def rm_thread(self):
        # Use port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            print(e)
            os.close(1)


        while True:
            try:
                data,_ = s.recvfrom(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)
                print(YELLOW + "(RECV) -> RM: "+ str(data) + RESET)

                if (data["type"] == "all_replicas" or data["type"] == "add_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip in self.members:
                            print(RED + "Received add_replicas ip (" + replica_ip + ") that was already in membership set" + RESET)
                        else: 
                            self.members[replica_ip] = None
                    self.members_mutex.release()

                    if self.ip == data["primary"]:
                        self.is_primary = True
                    # else:
                    #     self.primary_ip = data["primary"]

                    if self.ip in data["ip_list"]: # First connected as new member, need to get state from other members
                        self.members_mutex.acquire()
                        del self.members[self.ip]
                        self.members_mutex.release()
                        print(RED + "Saw own IP. Getting checkpoint from other replicas" + RESET) 
                        self.get_connection_from_old_replicas()

                    else: # Already member, need to connect to new members
                        time.sleep(1) # delay to allow new members to start listening for connection
                        self.connect_to_new_replicas()

                elif (data["type"] == "del_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip not in self.members:
                            print(RED + "Received del_replicas ip that was not in membership set" + RESET)
                        else:
                            if (self.members[replica_ip] != None):
                                self.members[replica_ip].close() # close the socket to the failed replica
                            del self.members[replica_ip]

                            # Set primary_ip to None, if the primary was deleted
                            # if (self.primary_ip == replica_ip):
                            #     self.primary_ip = None
                    self.members_mutex.release()

                    if self.ip == data["primary"]:
                        self.is_primary = True


                elif (data["type"] == "chkpt_freq"):
                    time_val = data["time"]
                    self.checkpoint_interval = time_val
                    print(GREEN + "Checkpoint Interval changed to: {} s".format(time_val) + RESET)


                else:
                    print(RED + "Received bad packet type from RM" + RESET)

                # Print out the new membership set
                members = [addr for addr in self.members] + [self.ip]
                print(RED + "Updated Membership Set: " +str(members) + RESET)

            except KeyboardInterrupt:
                s.close()
                return

    def missing_connections(self):
        for addr in self.members:
            if (self.members[addr] == None):
                return True
        return False

    def get_connection_from_old_replicas(self):
        # For a new Replica
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.replica_port))
        s.listen(5)
        self.members_mutex.acquire()
        try:
            while(self.missing_connections()):
                # Accept a new connection
                conn, addr = s.accept()
                addr = addr[0]
                self.members[addr] = conn

                print(RED + "Received connection from existing replica at" + addr + ":" + str(self.replica_port) + RESET)

                threading.Thread(target=self.checkpoint_receive_thread, args=(conn, addr, )).start()

        except Exception as e:
            s.close()
            print(e)

        self.members_mutex.release()
        self.good_to_go = True

    # Connect to any new replicas. If we are the primary, also send a checkpoint to each new replica
    def connect_to_new_replicas(self):
        # Running Replica
        if (self.is_primary):
            self.quiesce_lock.acquire()
            self.is_in_quiescence = True
            print(MAGENTA + "Quiescence start: sending initial checkpoint to new members" + RESET)

            for addr in self.members:
                if self.members[addr] == None:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        s.connect((addr, self.replica_port))
                        self.members_mutex.acquire()
                        self.members[addr] = s
                        self.members_mutex.release()
                        print(RED + "Connected to new replica at: " + addr + ":" + str(self.replica_port) + RESET)

                        # checkpointing
                        replica_ckpt = self.create_checkpoint()

                        try:
                            s.send(replica_ckpt.encode("utf-8"))
                        except:
                            print(RED + 'Replica ckeckpointing failed at:' + self.members[addr] + RESET)

                        print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                    except Exception as e:
                        s.close()
                        print(e)

            self.is_in_quiescence = False
            print(MAGENTA + "Quiescence end" + RESET)
            self.quiesce_lock.release()

        else:
            for addr in self.members:
                if self.members[addr] == None:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        s.connect((addr, self.replica_port))
                        self.members_mutex.acquire()
                        self.members[addr] = s
                        self.members_mutex.release()
                        print(RED + "Connected to new replica at: " + addr + ":" + str(self.replica_port) + RESET)

                    except Exception as e:
                        s.close()
                        print(e)

    
    ###############################################
    # Passive Replication functions
    ###############################################

    def send_checkpoint(self):
        self.quiesce_lock.acquire()
        print(MAGENTA + "Quiescence start: sending checkpoint" + RESET)
        self.is_in_quiescence = True
        checkpoint_msg = self.create_checkpoint()

        self.members_mutex.acquire()
        for addr in self.members:
            if self.members[addr] == None:
                # Ignore replicas we have yet to make a connection to
                # This may be the case if we send a checkpoint immediately after a membership change
                pass

            else:
                try:
                    s = self.members[addr]
                    s.send(checkpoint_msg.encode("utf-8"))

                except Exception as e:
                    print(RED + 'Replica ckeckpointing failed at:' + self.members[addr] + RESET)
                    print(e)

                print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, checkpoint_msg) + self.ip + RESET)
        self.members_mutex.release()

        self.is_in_quiescence = False
        print(MAGENTA + "Quiescence end" + RESET)
        self.quiesce_lock.release()

    # If we are the primary, broadcast a checkpoint out to all members every self.checkpoint_interval
    def checkpoint_send_thread(self):
        try:
            # Wait until we actually receive a checkpoint interval
            while(self.checkpoint_interval == None):
                pass

            while(1):
                if (self.is_primary):
                    self.send_checkpoint()

                    # Sleep for checkpoint_interval
                    time.sleep(self.checkpoint_interval)

        except KeyboardInterrupt:
            print(RED + "Checkpoint send thread terminated by KeyboardInterrupt" + RESET)
            return

    # If we are a backup, receive checkpoint and update state. 
    # One of these threads is created for every member, primary or not.
    # We assume only the primary will send a checkpoint message, while the backups remain silent.
    def checkpoint_receive_thread(self, s, addr):
        try:
            while(1):
                if (self.is_primary == False): #and (self.primary_ip != None):
                    # conn = self.members[self.primary_ip]
                    try:
                        data = s.recv(BUF_SIZE)
                        if data:
                            checkpoint_msg = json.loads(data.decode("utf-8"))
                            assert(checkpoint_msg["type"] == "checkpoint")

                            self.checkpoint_lock.acquire()
                            print(MAGENTA + 'Checkpoint received from {}: {}'.format(addr, checkpoint_msg) + self.ip + RESET)
            
                            self.main_msg_count = checkpoint_msg["main_msg_count"]
                            self.per_client_msg_count = checkpoint_msg["per_client_msg_count"]
                            self.ckpt_received = True

                            log_list = []
                            while(not self.client_msg_queue.empty()):
                                data = self.client_msg_queue.get()
                                username = data["username"]

                                # Solves the issue of the 
                                if username not in self.per_client_msg_count:
                                    # Delete user from the user dictionary
                                    if username in self.users:
                                        self.users_mutex.acquire()
                                        del self.users[username]
                                        self.users_mutex.release()
                                    del self.client_msg_dict[(username, data["clock"])]
                                    continue

                                # Ignore anything already processed as indicated by the checkpoint
                                if data["clock"] < self.per_client_msg_count[username]:
                                    del self.client_msg_dict[(username, data["clock"])]
                                    continue

                                log_list.append(data)
                            
                            # Write logs to log file. Use tail -f log.txt to print the logs.
                            # This allows the logs to be printed in a separate window, so as to
                            # not interfere with other message types
                            self.size_of_log = len(log_list)
                            print(GREEN + "Size of Log:" + str(self.size_of_log) + RESET)
                            
                            with open(self.log_file_name, 'w') as f:
                                for data in log_list:
                                    f.write(str(data))
                                    self.client_msg_queue.put(data)

                            self.checkpoint_lock.release()
                    except:
                        return

        except KeyboardInterrupt:
            print(RED + "Checkpoint receive thread terminated by KeyboardInterrupt" + RESET)
            return

    # Creates a checkpoint message
    def create_checkpoint(self):
        assert(self.is_in_quiescence)
        checkpoint_msg = {}
        checkpoint_msg["type"] = "checkpoint"
        checkpoint_msg["main_msg_count"] = self.main_msg_count
        checkpoint_msg["per_client_msg_count"] = self.per_client_msg_count

        checkpoint_msg = json.dumps(checkpoint_msg)
        return checkpoint_msg


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

    def client_message_receiving_thread(self, s, addr):
        # Client has connected to the server
        # We expect the first packet from the client to be a JSON login packet {"type": "login", "username":<username>}
        login_data = s.recv(BUF_SIZE)
        login_data = login_data.decode("utf-8")
        print(CYAN + str(login_data) + RESET)
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
        print(RED + "Accepted: {}".format(username) + RESET)
        # Add the client socket to the users dictionary
        self.users_mutex.acquire()
        self.users[username] = s
        self.users_mutex.release()

        if username not in self.per_client_msg_count:
            self.per_client_msg_count[username] = 0

        # Insert job in client queue
        while self.is_in_quiescence:
            print(GREEN + "Log: {}".format(login_data) + RESET)

        self.client_msg_dict[(username, login_data["clock"])] = login_data
        self.client_msg_queue.put(login_data)

        
        # Receive, process, and retransmit chat messages from the client
        try:
            while True:
                try:
                    data = s.recv(BUF_SIZE)
                    if data:
                        self.checkpoint_lock.acquire()
                        data = data.decode("utf-8")
                        data = json.loads(data)

                        while self.is_in_quiescence: #Note that is_in_quiescence should only by true when we are the primary 
                            print(GREEN + "Log: {}".format(data) + RESET)


                        self.client_msg_dict[(username, data["clock"])] = data
                        self.client_msg_queue.put(data)

                        with open(self.log_file_name, 'a') as f:
                            f.write(str(data))

                        if (not self.is_primary):
                            self.size_of_log = self.size_of_log + 1
                            print(GREEN + "Size of Log:" + str(self.size_of_log) + RESET)

                        self.checkpoint_lock.release()

                except:
                    print(RED + "{} has disconnected".format(username) + RESET)
                    s.close()
                    return
        except KeyboardInterrupt:
            print(RED + "Client msg receive thread terminated by KeyboardInterrupt" + RESET)
            return



    def client_message_processing_thread(self):
        try:
            while True:
                # Primary - process messages
                if (self.is_primary):
                    self.quiesce_lock.acquire()

                    # Get job from the queue and process it
                    if self.client_msg_queue.empty():
                        self.quiesce_lock.release()
                        continue
                    
                    # Pop a message from the queue
                    data = self.client_msg_queue.get()
                    username = data["username"]

                    del self.client_msg_dict[(username, data["clock"])]

                    # # If the message has already been processed
                    # # James: Is this check necessary in passive? Probably not I think.
                    # if data["clock"] < self.per_client_msg_count[username]:
                    #     self.quiesce_lock.release()
                    #     continue
                    
                    self.per_client_msg_count[username] += 1

                    # Print received message here
                    print(YELLOW + "(PROC) -> {}".format(data) + RESET)
                    
                    # Login Packet
                    if (data["type"] == "login"):
                        # Send user joined message to all other users
                        message = dict()
                        message["type"] = "login_success"
                        message["username"] = data["username"]
                        message["clock"] = self.main_msg_count
                        message = json.dumps(message)
                        self.broadcast(message)
                        self.main_msg_count += 1

                    # If the client is attempting to logout
                    if (data["type"] == "logout"):
                        s = self.users[username]
                        # Delete the current client from the dictionary
                        self.users_mutex.acquire()
                        del self.users[username]
                        self.users_mutex.release()

                        del self.per_client_msg_count[username]

                        print(RED + "Logout from:", username + RESET)

                        message = dict()
                        message["type"] = "logout_success"
                        message["username"] = username
                        message["clock"] = self.main_msg_count
                        message = json.dumps(message)
                        self.broadcast(message)
                        self.main_msg_count += 1

                        s.close()

                    # If the client sends a normal chat message
                    elif (data["type"] == "send_message"):
                        chat_message = data["text"]

                        message = dict()
                        message["type"] = "receive_message"
                        message["username"] = username
                        message["text"] = chat_message
                        message["clock"] = self.main_msg_count

                        message = json.dumps(message)
                        self.broadcast(message)
                        self.main_msg_count += 1

                    self.quiesce_lock.release()


        except KeyboardInterrupt:
            print(RED + "Client msg processing thread terminated by KeyboardInterrupt" + RESET)
            return

        
    def chat_server(self):
        # Open listening socket of Replica
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.client_port))
        s.listen(5)

        try:
            while(True):
                # Accept a new connection
                conn, addr = s.accept()
                print("Accepted new client")
                # Initiate a client listening thread
                threading.Thread(target=self.client_message_receiving_thread, args=(conn, addr), daemon=True).start()

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
    sys.exit(1)