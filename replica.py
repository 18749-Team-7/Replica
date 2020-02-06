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

    def __init__(self, replication_type, verbose=True):
        self.replication_type = replication_type

        if self.replication_type == "active":
            self.set_host_ip_active()
            self.ip = self.host_ip
            self.verbose = verbose
            
            self.client_port  = 5000
            self.HB_port      = 10000
            self.RM_port      = 15000
            self.replica_port = 20000

            # Queues and Dicts
            self.rp_msg_count = 0
            self.client_msg_queue = multiprocessing.Queue()
            self.manager = multiprocessing.Manager()
            self.client_msg_dict = self.manager.dict()
            self.per_client_msg_count = {}
            self.is_in_quiescence = True
            self.quiesce_lock = threading.Lock()

            # Total order data structures
            self.votes = {}

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

            # Start the heartbeat thread
            self.hb_freq = 1
            self.start_heartbeat_active() # TODO: Don't hardcode these values. Interval = 1 sec

            # Start the RM thread
            # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
            # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
            threading.Thread(target=self.rm_thread_active, daemon=True).start()



            # Start the chat server
            print(MAGENTA + "Quiescence start" + RESET)
            # threading.Thread(target=self.print_membership_thread,args=(1,)).start()
            print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
            threading.Thread(target=self.client_msg_queue_proc_active, daemon=True).start()
            self.chat_server_active()
        
        else:
            self.set_host_ip_passive()
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
            self.start_heartbeat_passive(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

            # Start the RM thread
            # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
            # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
            # It will also start checkpoint_receive_threads for each other replica
            threading.Thread(target=self.rm_thread_passive, daemon=True).start()

            # Start the checkpoint_send_thread
            threading.Thread(target=self.checkpoint_send_thread_passive, daemon=True).start()

            # Start the chat server
            print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
            threading.Thread(target=self.client_message_processing_thread_passive, daemon=True).start()
            self.chat_server_passive()

    ###############################################
    # Active Replication Supporting functions
    ###############################################

    def set_host_ip_active(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    def print_exception_active(self):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    ###############################################
    # Heartbeat functions
    ###############################################
    def heartbeat_thread_active(self, s):
        while(True):
            try:
                packet = {}
                packet["type"] = "heartbeat"
                packet["time"] = self.hb_freq
                packet = json.dumps(packet)
                s.send(packet.encode("utf-8"))

                time.sleep(self.hb_freq)

            except KeyboardInterrupt:
                s.close()
                return

            except Exception as e:
                self.print_exception_active()
                time.sleep(self.hb_freq)

    def start_heartbeat_active(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            self.print_exception_active()
            return

        threading.Thread(target=self.heartbeat_thread_active,args=(s, ), daemon=True).start()


    ###############################################
    # Replica Membership functions
    ###############################################
    def print_membership_thread(self, interval):
        while True:
            members = [addr for addr in self.members] + [self.ip]
            print("Current Membership Set:" +str(members))
            time.sleep(interval)

    def rm_thread_active(self):
        # Use port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            self.print_exception_active()
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

                    if self.ip in data["ip_list"]: # First connected as new member, need to get state from other members
                        self.members_mutex.acquire()
                        del self.members[self.ip]
                        self.members_mutex.release()
                        print(RED + "Saw own IP. Getting checkpoint from other replicas" + RESET) 
                        self.get_connection_from_old_replicas_active()

                    else: # Already member, need to connect to new members
                        time.sleep(1)
                        self.connect_to_new_replicas_active()

                elif (data["type"] == "del_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip not in self.members:
                            print(RED + "Received del_replicas ip that was not in membership set" + RESET)
                        else:
                            if (self.members[replica_ip] != None):
                                self.members[replica_ip].close() # close the socket to the failed replica
                            del self.members[replica_ip]
                    self.members_mutex.release()

                elif (data["type"] == "chkpt_freq"):
                    time_val = data["time"]
                    print(RED + "Changed heartbeat interval to:" +str(time_val) + "s" + RESET)
                    self.hb_freq = time_val
                
                elif (data["type"] == "replication_type"):
                    replica_type = data["replication"]
                    print(RED + "Changed Replication to:" +str(replica_type) + RESET)
                    self.replication_type = replica_type

                else:
                    print(RED + "Received bad packet type from RM" + RESET)

                # Print out the new membership set
                members = [addr for addr in self.members] + [self.ip]
                print(RED + "Updated Membership Set: " +str(members) + RESET)

            except KeyboardInterrupt:
                s.close()
                return

    def missing_connections_active(self):
        for addr in self.members:
            if (self.members[addr] == None):
                return True
        return False

    def get_connection_from_old_replicas_active(self):
        # For a new Replica

        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.replica_port))
        s.listen(5)
        self.members_mutex.acquire()
        try:
            while(self.missing_connections_active()):
                # Accept a new connection
                conn, addr = s.accept()
                addr = addr[0]
                self.members[addr] = conn

                try:
                    data = conn.recv(BUF_SIZE)
                    if data:
                        if not self.ckpt_received:
                            replica_ckpt = json.loads(data.decode("utf-8"))

                            print(MAGENTA + 'Checkpoint received from {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                            assert(replica_ckpt["type"] == "checkpoint")
                            self.rp_msg_count = replica_ckpt["rp_msg_count"]
                            self.per_client_msg_count = replica_ckpt["per_client_msg_count"]
                            self.ckpt_received = True
                        else:
                            assert(replica_ckpt["type"] == "checkpoint")
                            checkpoint_msg = {}
                            checkpoint_msg["type"] = "checkpoint"
                            checkpoint_msg["rp_msg_count"] = self.rp_msg_count
                            checkpoint_msg["per_client_msg_count"] = self.per_client_msg_count
                            print(MAGENTA + "Internal State: {}".format(checkpoint_msg) + RESET)
                            print(MAGENTA + "Checkpoint {}: {}".format(addr, checkpoint_msg) + RESET)
                            
  
                except KeyboardInterrupt:
                    s.close()
                    return

                
                print(RED + "Received connection from existing replica at" + addr + ":" + str(self.replica_port) + RESET)

                threading.Thread(target=self.replica_receive_thread_active,args=(conn, addr), daemon=True).start()

            self.is_in_quiescence = False
            print(MAGENTA + "Quiescence end" + RESET)
     
        except KeyboardInterrupt:
            s.close()
            return

        except Exception as e:
            s.close()
            self.print_exception_active()

        self.members_mutex.release()
        self.good_to_go = True

    def connect_to_new_replicas_active(self):
        # Running Replica

        self.quiesce_lock.acquire()
        self.is_in_quiescence = True

        print(MAGENTA + "Quiescence start" + RESET)

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
                    # print(s)

                    # checkpointing
                    replica_ckpt = self.create_checkpoint_active()

                    try:
                        s.send(replica_ckpt.encode("utf-8"))
                    except:
                        print(RED + 'Replica ckeckpointing failed at:' + self.members[addr] + RESET)

                    print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                    threading.Thread(target=self.replica_receive_thread_active,args=(s, addr)).start()

                except KeyboardInterrupt:
                    s.close()
                    return

                except Exception as e:
                    s.close()
                    self.print_exception_active()

        self.is_in_quiescence = False
        print(MAGENTA + "Quiescence end" + RESET)
        self.quiesce_lock.release()


    # Constantly receive votes from other replicas
    def replica_receive_thread_active(self, s, addr):
        try:
            while True:
                data = s.recv(BUF_SIZE)
                if data:
                    data = data.decode("utf-8")
                    data = json.loads(data)

                    if (data["type"] == "vote"):
                        message = data["text"]
                        self.votes[addr] = message


                    else:
                        print(RED + "Malformed Vote Packet: "+ data["type"] + RESET)
        except KeyboardInterrupt:
            s.close()
            return
        except Exception as e:
            self.print_exception_active()
            return
    
    ###############################################
    # Chat client functions
    ###############################################
    def broadcast_active(self, message):
        # send message to all clients
        self.users_mutex.acquire()
        for _, s_client in self.users.items():
            try:
                s_client.send(message.encode("utf-8"))
            except:
                pass
        self.users_mutex.release()
        return

    def get_hash_active(self, message, count):
        return hashlib.sha256(message + str(count)).hexdigest()

    def client_service_thread_active(self, s, addr):
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
            print(RED + "Client sent malformed packet, closing socket" + RESET)
            s.close()
            return 

        if (login_data["username"] in self.users): # Username already in use
            # Send failed login packet to new user
            message = dict()
            message["type"] = "error"
            message["text"] = "Username taken"
            message = json.dumps(message)
            s.send(message.encode("utf-8"))
            print(RED + "Client username already in use, closing socket" + RESET)
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
        if self.is_in_quiescence:
            print(GREEN + "Log: {}".format(login_data) + RESET)

        self.client_msg_dict[(username, login_data["clock"])] = login_data
        self.client_msg_queue.put(login_data)

        
        # Receive, process, and retransmit chat messages from the client
        raw_data = ''
        while True:
            try:
                raw_data += s.recv(BUF_SIZE).decode("utf-8")

                while (True): # Fix for Extra Data
                    start = raw_data.find("{")
                    end = raw_data.find("}") 
                    if start==-1 or end==-1:   # if can not find both { and } in string
                        break
                    data=json.loads(raw_data[start:end+1])  # only read { ... } and not another uncompleted data

                    if self.is_in_quiescence:
                        print(GREEN + "Log: {}".format(data) + RESET)
                    self.client_msg_dict[(username, data["clock"])] = data
                    self.client_msg_queue.put(data)

                    raw_data = raw_data[end+1:]

                # data = json.loads(data)

                # if self.is_in_quiescence:
                #     print(GREEN + "Log: {}".format(data) + RESET)

                # self.client_msg_dict[(username, data["clock"])] = data
                # self.client_msg_queue.put(data)

            except Exception as e:
                self.print_exception_active()

                print(RED + "{} has disconnected".format(username) + RESET)
                s.close()
                return
        return

    # Creates a checkpoint dictionary and returns it
    def create_checkpoint_active(self):
        checkpoint_msg = {}
        checkpoint_msg["type"] = "checkpoint"
        checkpoint_msg["rp_msg_count"] = self.rp_msg_count
        checkpoint_msg["per_client_msg_count"] = self.per_client_msg_count

        checkpoint_msg = json.dumps(checkpoint_msg)
        return checkpoint_msg
    
    # We pop the first message out of the queue and broadcast the vote to all replicas
    # We also place our own vote into our self.vote dict.
    def broadcast_vote_active(self, message):
        vote_msg = {}
        vote_msg["type"] = "vote"
        vote_msg["text"] = message
        vote_msg = json.dumps(vote_msg)

        self.members_mutex.acquire()
        for _, s_replica in self.members.items():
            if (s_replica != None):
                try:
                    s_replica.send(vote_msg.encode("utf-8"))
                except:
                    pass
        self.members_mutex.release()

        self.votes[self.host_ip] = message
        pass

    # Votes are read by the replica_receive_thread and place them into a set. 
    # Wait.
    # Once the set has been filled with enough votes (num replicas - 1), we commit the message and reset the set.
    def process_votes_active(self):
        # Make sure every member has a vote. If so, we break and count the votes. Else, we keep waiting.
        continue_flag = True
        while(continue_flag):

            continue_flag = False
            self.members_mutex.acquire()
            for addr in self.members: 
                if self.members[addr] == None:
                    continue
                if addr not in self.votes :
                    continue_flag = True
            self.members_mutex.release()

            if self.host_ip not in self.votes:
                continue_flag = True
        
        # If only one replica is alive, do not go in for vote counting
        if len(self.votes) != 1:
            # Count votes
            for addr1, message1 in self.votes.items():
                for addr2, message2 in self.votes.items():
                    if addr1 == addr2: 
                        pass
                    else:
                        if message1 == message2:
                            winning_message = message1
                            num_votes = len(self.votes)
                            print(CYAN + "Concensus achieved between " + str(num_votes) + " replicas: " + str(winning_message) + RESET)
                            self.votes.clear()

                            return winning_message

        # If no majority, force one based on list() method
        winning_replica = sorted(self.votes)
        winning_replica = winning_replica.pop(0)
        winning_message = self.votes[winning_replica]
        num_votes = len(self.votes)
        print(CYAN + "Concensus achieved between " + str(num_votes) + " replicas: " + str(winning_message) + RESET)
        self.votes.clear()

        return winning_message

    # Send message to client
    # Update client counts
    # Update global count
    def commit_message(self):
        pass


    def client_msg_queue_proc_active(self):
        while True:
            # Quiescence control added here
            
            while self.is_in_quiescence:
                continue

            # Get job from the queue and process it
            if self.client_msg_queue.empty():
                continue
            
            self.quiesce_lock.acquire()

            # Pop a message from the queue
            data = self.client_msg_queue.get()
            username = data["username"]

            # If the message has already been processed
            if username not in self.per_client_msg_count:
                self.quiesce_lock.release()
                continue


            if data["clock"] < self.per_client_msg_count[username]:
                # del self.client_msg_dict[(username, data["clock"])]
                self.quiesce_lock.release()
                continue

            self.broadcast_vote_active(data)

            data = self.process_votes_active()
            username = data["username"]

            self.per_client_msg_count[username] += 1

            # Print received message here
            print(YELLOW + "(PROC) -> {}".format(data) + RESET)
            
            # Login Packet
            if (data["type"] == "login"):
                # Send user joined message to all other users
                message = dict()
                message["type"] = "login_success"
                message["username"] = data["username"]
                message["clock"] = self.rp_msg_count
                message = json.dumps(message)
                self.broadcast_active(message)
                self.rp_msg_count += 1

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
                message["clock"] = self.rp_msg_count
                message = json.dumps(message)
                self.broadcast_active(message)
                self.rp_msg_count += 1

                print(RED + "Client logging out, closing socket" + RESET)
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
                self.broadcast_active(message)
                self.rp_msg_count += 1

            self.quiesce_lock.release()
            time.sleep(0.2)
    
    def chat_server_active(self):
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
                threading.Thread(target=self.client_service_thread_active, args=(conn, addr), daemon=True).start()

        except KeyboardInterrupt:
            self.users_mutex.acquire()
            for _, s_client in self.users.items():
                s_client.close()
            self.users_mutex.release()
            s.close()
            print(RED + "Closing chat server on " + str(self.ip) + ":" + str(self.client_port) + RESET)
        except Exception as e:
            self.print_exception_active()

    ###############################################
        # Passive Replication Supporting functions
    ###############################################

    def set_host_ip_passive(self):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect(("8.8.8.8", 80))
            self.host_ip = s.getsockname()[0]

    def print_exception_passive(self):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    ###############################################
    # Heartbeat functions
    ###############################################
    def heartbeat_thread_passive(self, s, interval):
        while(True):
            try:
                packet = '{"type": "heartbeat"}'
                s.send(packet.encode("utf-8"))
                time.sleep(interval)

            except KeyboardInterrupt:
                s.close()
                return

            except Exception as e:
                #self.print_exception_passive()
                time.sleep(interval)

    def start_heartbeat_passive(self, interval):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            self.print_exception_passive()
            return

        threading.Thread(target=self.heartbeat_thread_passive,args=(s, interval), daemon=True).start()


    ###############################################
    # Replica Membership functions
    ###############################################
    def rm_thread_passive(self):
        # Use port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            self.print_exception_passive()
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
                        self.get_connection_from_old_replicas_passive()

                    else: # Already member, need to connect to new members
                        time.sleep(1) # delay to allow new members to start listening for connection
                        self.connect_to_new_replicas_passive()

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

    def missing_connections_passive(self):
        for addr in self.members:
            if (self.members[addr] == None):
                return True
        return False

    def get_connection_from_old_replicas_passive(self):
        # For a new Replica
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.replica_port))
        s.listen(5)
        self.members_mutex.acquire()
        try:
            while(self.missing_connections_passive()):
                # Accept a new connection
                conn, addr = s.accept()
                addr = addr[0]
                self.members[addr] = conn

                print(RED + "Received connection from existing replica at" + addr + ":" + str(self.replica_port) + RESET)

                threading.Thread(target=self.checkpoint_receive_thread_passive, args=(conn, addr, )).start()

        except Exception as e:
            s.close()
            self.print_exception_passive()

        self.members_mutex.release()
        self.good_to_go = True

    # Connect to any new replicas. If we are the primary, also send a checkpoint to each new replica
    def connect_to_new_replicas_passive(self):
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
                        replica_ckpt = self.create_checkpoint_passive()

                        try:
                            s.send(replica_ckpt.encode("utf-8"))
                        except:
                            print(RED + 'Replica ckeckpointing failed at:' + self.members[addr] + RESET)

                        print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                        threading.Thread(target=self.checkpoint_receive_thread_passive, args=(s, addr, )).start()

                    except Exception as e:
                        s.close()
                        self.print_exception_passive()

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

                        threading.Thread(target=self.checkpoint_receive_thread_passive, args=(s, addr, )).start()

                    except Exception as e:
                        s.close()
                        self.print_exception_passive()


    ###############################################
    # Passive Replication functions
    ###############################################

    def send_checkpoint_passive(self):
        self.quiesce_lock.acquire()
        print(MAGENTA + "Quiescence start: sending checkpoint" + RESET)
        self.is_in_quiescence = True
        checkpoint_msg = self.create_checkpoint_passive()

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
                    self.print_exception_passive()

                print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, checkpoint_msg) + self.ip + RESET)
        self.members_mutex.release()

        self.is_in_quiescence = False
        print(MAGENTA + "Quiescence end" + RESET)
        self.quiesce_lock.release()

    # If we are the primary, broadcast a checkpoint out to all members every self.checkpoint_interval
    def checkpoint_send_thread_passive(self):
        try:
            # Wait until we actually receive a checkpoint interval
            while(self.checkpoint_interval == None):
                pass

            while(1):
                if (self.is_primary):
                    self.send_checkpoint_passive()

                    # Sleep for checkpoint_interval
                    time.sleep(self.checkpoint_interval)

        except KeyboardInterrupt:
            print(RED + "Checkpoint send thread terminated by KeyboardInterrupt" + RESET)
            return

    # If we are a backup, receive checkpoint and update state. 
    # One of these threads is created for every member, primary or not.
    # We assume only the primary will send a checkpoint message, while the backups remain silent.
    def checkpoint_receive_thread_passive(self, s, addr):
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
    def create_checkpoint_passive(self):
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
    def broadcast_passive(self, message):
        # send message to all clients
        self.users_mutex.acquire()
        for _, s_client in self.users.items():
            s_client.send(message.encode("utf-8"))
        self.users_mutex.release()
        return

    def get_hash(self, message, count):
        return hashlib.sha256(message + str(count)).hexdigest()

    def client_message_receiving_thread_passive(self, s, addr):
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
            print(RED + "Client sent malformed packet, closing socket" + RESET)
            s.close()
            return 

        if (login_data["username"] in self.users): # Username already in use
            # Send failed login packet to new user
            message = dict()
            message["type"] = "error"
            message["text"] = "Username taken"
            message = json.dumps(message)
            s.send(message.encode("utf-8"))
            print(RED + "Client username already in use, closing socket" + RESET)
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
            raw_data = ''
            while True:
                try:
                    raw_data += s.recv(BUF_SIZE).decode("utf-8")
                    if raw_data:

                        while (True): # Fix for Extra Data
                            start = raw_data.find("{")
                            end = raw_data.find("}") 
                            if start==-1 or end==-1:   # if can not find both { and } in string
                                break
                            data=json.loads(raw_data[start:end+1])  # only read { ... } and not another uncompleted data

                            self.checkpoint_lock.acquire()

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

                            raw_data = raw_data[end+1:]

                        

                except:
                    self.print_exception_passive()
                    print(RED + "{} has disconnected".format(username) + RESET)
                    s.close()
                    return
        except KeyboardInterrupt:
            print(RED + "Client msg receive thread terminated by KeyboardInterrupt" + RESET)
            return



    def client_message_processing_thread_passive(self):
        try:
            while True:
                # Primary - process messages
                if (self.is_primary):

                    # Get job from the queue and process it
                    if self.client_msg_queue.empty():
                        continue

                    self.quiesce_lock.acquire()

                    
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
                        self.broadcast_passive(message)
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
                        self.broadcast_passive(message)
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
                        self.broadcast_passive(message)
                        self.main_msg_count += 1

                    self.quiesce_lock.release()


        except KeyboardInterrupt:
            print(RED + "Client msg processing thread terminated by KeyboardInterrupt" + RESET)
            return

        
    def chat_server_passive(self):
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
                threading.Thread(target=self.client_message_receiving_thread_passive, args=(conn, addr), daemon=True).start()

        except KeyboardInterrupt:
            self.users_mutex.acquire()
            for _, s_client in self.users.items():
                s_client.close()
            self.users_mutex.release()
            s.close()
            print(RED + "Closing chat server on " + str(self.ip) + ":" + str(self.client_port) + RESET)
        except Exception as e:
            self.print_exception_passive()




def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-t', '--replication_type', help="Replication Type", default="active")
    parser.add_argument('-v', '--verbose', help="Print every chat message", action='store_true')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()

    args = get_args()
    replica_obj = Replica(args.replication_type,args.verbose)

    print("\nTotal time taken: " + str(time.time() - start_time) + " seconds")

    # Exit
    sys.exit(1)