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
        self.rp_msg_count = 0
        self.client_msg_queue = multiprocessing.Queue()
        self.manager = multiprocessing.Manager()
        self.client_msg_dict = self.manager.dict()
        self.per_client_msg_count = {}
        self.is_in_quiescence = True

        # Total order data structures
        self.votes = self.manager.dict()

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
        self.start_heartbeat(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

        # Start the RM thread
        # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
        # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
        threading.Thread(target=self.rm_thread, daemon=True).start()



        # Start the chat server
        print(MAGENTA + "Quiescence start" + RESET)
        # threading.Thread(target=self.print_membership_thread,args=(1,)).start()
        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
        threading.Thread(target=self.client_msg_queue_proc, daemon=True).start()
        self.chat_server()




    def set_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    def print_exception(self):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

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
                self.print_exception()
                time.sleep(interval)

    def start_heartbeat(self, interval):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            self.print_exception()
            return

        threading.Thread(target=self.heartbeat_thread,args=(s, interval), daemon=True).start()


    ###############################################
    # Replica Membership functions
    ###############################################
    def print_membership_thread(self, interval):
        while True:
            members = [addr for addr in self.members] + [self.ip]
            print("Current Membership Set:" +str(members))
            time.sleep(interval)

    def rm_thread(self):
        # Use port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            self.print_exception()
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
                        self.get_connection_from_old_replicas()

                    else: # Already member, need to connect to new members
                        time.sleep(1)
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
                    self.members_mutex.release()

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

                threading.Thread(target=self.replica_receive_thread,args=(conn, addr), daemon=True).start()

            self.is_in_quiescence = False
            print(MAGENTA + "Quiescence end" + RESET)
     
        except KeyboardInterrupt:
            s.close()
            return

        except Exception as e:
            s.close()
            self.print_exception()

        self.members_mutex.release()
        self.good_to_go = True

    def connect_to_new_replicas(self):
        # Running Replica

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
                    replica_ckpt = self.create_checkpoint()

                    try:
                        s.send(replica_ckpt.encode("utf-8"))
                    except:
                        print(RED + 'Replica ckeckpointing failed at:' + self.members[addr] + RESET)

                    print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                    threading.Thread(target=self.replica_receive_thread,args=(s, addr)).start()

                except KeyboardInterrupt:
                    s.close()
                    return

                except Exception as e:
                    s.close()
                    self.print_exception()

        self.is_in_quiescence = False
        print(MAGENTA + "Quiescence end" + RESET)


    # Constantly receive votes from other replicas
    def replica_receive_thread(self, s, addr):
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
            self.print_exception()
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
                self.print_exception()

                print(RED + "{} has disconnected".format(username) + RESET)
                s.close()
                return
        return

    # Creates a checkpoint dictionary and returns it
    def create_checkpoint(self):
        checkpoint_msg = {}
        checkpoint_msg["type"] = "checkpoint"
        checkpoint_msg["rp_msg_count"] = self.rp_msg_count
        checkpoint_msg["per_client_msg_count"] = self.per_client_msg_count

        checkpoint_msg = json.dumps(checkpoint_msg)
        return checkpoint_msg
    
    # We pop the first message out of the queue and broadcast the vote to all replicas
    # We also place our own vote into our self.vote dict.
    def broadcast_vote(self, message):
        vote_msg = {}
        vote_msg["type"] = "vote"
        vote_msg["text"] = message
        vote_msg = json.dumps(vote_msg)

        self.members_mutex.acquire()
        for _, s_replica in self.members.items():
            if (s_replica != None):
                s_replica.send(vote_msg.encode("utf-8"))
        self.members_mutex.release()

        self.votes[self.host_ip] = message
        pass

    # Votes are read by the replica_receive_thread and place them into a set. 
    # Wait.
    # Once the set has been filled with enough votes (num replicas - 1), we commit the message and reset the set.
    def process_votes(self):
        # Make sure every member has a vote. If so, we break and count the votes. Else, we keep waiting.
        while(True):

            self.members_mutex.acquire()
            for addr in self.members: 
                if addr not in self.votes:
                    continue
            self.members_mutex.release()

            if self.host_ip not in self.votes:
                continue

            break
        
        # Count votes
        for addr1, message1 in self.votes:
            for addr2, message2 in self.votes:
                if addr1 == addr2: 
                    pass
                else:
                    if message1 == message2:
                        winning_message = message1
                        num_votes = len(self.votes)
                        print(CYAN + "Concensus achieved between " + str(num_votes) + " replicas: " + str(winning_message) + RESET)
                        self.votes = {}

                        return winning_message

        # If no majority, force one based on sorted() method
        winning_replica = sorted(self.votes)[0]
        winning_message = self.votes[winning_replica]
        num_votes = len(self.votes)
        print(CYAN + "Concensus achieved between " + str(num_votes) + " replicas: " + str(winning_message) + RESET)
        self.votes = {}

        return winning_message

    # Send message to client
    # Update client counts
    # Update global count
    def commit_message(self):
        pass


    def client_msg_queue_proc(self):
        while True:
            # Quiescence control added here
            while self.is_in_quiescence:
                continue

            # Get job from the queue and process it
            if self.client_msg_queue.empty():
                continue
            
            # Pop a message from the queue
            data = self.client_msg_queue.get()
            username = data["username"]

            # If the message has already been processed
            if data["clock"] < self.per_client_msg_count[username]:
                del self.client_msg_dict[(username, data["clock"])]
                continue

            self.broadcast_vote(data)

            data = self.process_votes()
            username = data["username"]

            if data["clock"] < self.per_client_msg_count[username]:
                print(RED + "ERROR: this should not happen" + RESET)


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
                self.broadcast(message)
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
                self.broadcast(message)
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
                self.broadcast(message)
                self.rp_msg_count += 1
    
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
                threading.Thread(target=self.client_service_thread, args=(conn, addr), daemon=True).start()

        except KeyboardInterrupt:
            self.users_mutex.acquire()
            for _, s_client in self.users.items():
                s_client.close()
            self.users_mutex.release()
            s.close()
            print(RED + "Closing chat server on " + str(self.ip) + ":" + str(self.client_port) + RESET)
        except Exception as e:
            self.print_exception()


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