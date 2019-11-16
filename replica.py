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
        self.client_proc_msg_count = {}
        self.is_in_quiescence = True
        self.majority = 0
        self.votes = dict()
        self.current_proposal =dict()

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
        self.checkpoint_mutex = threading.Lock() # Lock on the checkpoint creation
        self.votes_mutex = threading.Lock() #Lock on votes 

        # Start the heartbeat thread
        self.start_heartbeat(interval=1) # TODO: Don't hardcode these values. Interval = 1 sec

        # Start the RM thread
        # Upon startup, this Replica will receive the add_replicas packet with its own IP. 
        # It will initiate get_connection_from_old_replicas() to get up to date with the other replicas
        threading.Thread(target=self.rm_thread, daemon=True).start()


        while (not self.good_to_go):
            pass

        # Start the chat server
        print(GREEN + "Good to Go" + RESET)
        # threading.Thread(target=self.print_membership_thread,args=(1,)).start()
        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
        threading.Thread(target=self.client_msg_queue_proc, daemon=True).start()
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
            print(e)
            os.close(1)


        while True:
            try:
                data,_ = s.recvfrom(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)
                print(data)


                if (data["type"] == "all_replicas" or data["type"] == "add_replicas" ):
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
                        # data = MAGENTA + replica_ckpt['rp_msg_count'] + replica_ckpt['client_proc_msg_count'] + self.ip + RESET

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
                print("Updated Membership Set: " +str(members))

            except KeyboardInterrupt:
                s.close()
                return

    def missing_connections(self):
        print(self.members)
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
                    data = s.recv(BUF_SIZE)
                    if data:
                        if not self.ckpt_received:
                            replica_ckpt = json.loads(data.decode("utf-8"))

                            print(MAGENTA + 'Replica received {}'.format(replica_ckpt) + self.ip + RESET)

                            assert(replica_ckpt["type"] == "checkpoint")
                            self.rp_msg_count = replica_ckpt["rp_msg_count"]
                            self.client_proc_msg_count = replica_ckpt["client_proc_msg_count"]
                            self.ckpt_received = True
                        else:
                            assert(replica_ckpt["type"] == "checkpoint")
                            checkpoint_msg = {}
                            checkpoint_msg["type"] = "checkpoint"
                            checkpoint_msg["rp_msg_count"] = self.rp_msg_count
                            checkpoint_msg["client_proc_msg_count"] = self.client_proc_msg_count
                            print(MAGENTA + "Internal State: {}".format(self.checkpoint_msg) + RESET)
                            print(MAGENTA + "Checkpoint {}: {}".format(addr, self.checkpoint_msg) + RESET)
                            
  
                except KeyboardInterrupt:
                    s.close()
                    return

                
                print(RED + "Received connection from existing replica at" + addr + ":" + str(self.replica_port) + RESET)
                # threading.Thread(target=self.replica_send_thread,args=(conn,), daemon=True).start()
                threading.Thread(target=self.replica_receive_thread,args=(conn,addr), daemon=True).start()

            self.is_in_quiescence = False
     
        except KeyboardInterrupt:
            s.close()
            return

        except Exception as e:
            s.close()
            print(e)

        self.members_mutex.release()
        self.good_to_go = True

    def connect_to_new_replicas(self):
        # Running Replica

        self.is_in_quiescence = True

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
                    print(s)

                    # checkpointing
                    self.checkpoint_mutex.acquire()
                    replica_ckpt = self.create_checkpoint()
                    self.checkpoint_mutex.release()
                    try:
                        s.send(replica_ckpt).encode("utf-8")
                    except:
                        print('Replica ckeckpointing failed at:' + self.members[addr])

    
                    threading.Thread(target=self.replica_receive_thread,args=(s, addr)).start()

                except KeyboardInterrupt:
                    s.close()
                    return

                except Exception as e:
                    s.close()
                    print(e)

        self.is_in_quiescence = False


    def replica_send_thread(self, s):
        replica_to_replica_count = 0
        while True:
            try:
                data = YELLOW + "Ping from " + self.ip + " | " + str(replica_to_replica_count) + RESET
                s.send(data.encode("utf-8"))
                replica_to_replica_count = replica_to_replica_count + 1
                time.sleep(1)

            except KeyboardInterrupt:
                s.close()
                return
            except Exception as e:
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
        print(RED + "Accepted: {}".format(username) + RESET)
        # Add the client socket to the users dictionary
        self.users_mutex.acquire()
        self.users[username] = s
        self.users_mutex.release()

        if username not in self.client_proc_msg_count:
            self.client_proc_msg_count[username] = 0

        # Insert job in client queue
        self.client_msg_dict[(username, login_data["clock"])] = login_data
        self.client_msg_queue.put(login_data)

        
        # Receive, process, and retransmit chat messages from the client
        while True:
            try:
                data = s.recv(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)

                self.client_msg_dict[(username, data["clock"])] = data
                self.client_msg_queue.put(data)

            except:
                print(RED + "{} has disconnected".format(username) + RESET)
                s.close()
                return
        return

    # Creates a checkpoint dictionary and returns it
    def create_checkpoint(self):
        checkpoint_msg = {}
        checkpoint_msg["type"] = "checkpoint"
        checkpoint_msg["rp_msg_count"] = self.rp_msg_count
        checkpoint_msg["client_proc_msg_count"] = self.client_proc_msg_count

        checkpoint_msg = json.loads(checkpoint_msg)
        return checkpoint_msg

    def broadcast_votes(self):
        # send proposals to all other replicas 
        self.members_mutex.acquire()
        for addr in self.members:
            if self.members[addr] != None:
                try: 
                    self.members[addr].send(self.current_proposal.encode('utf-8'))
                except Exception as e:
                    self.members[addr].close()
                    continue
        self.members_mutex.release()


    def replica_receive_thread(self, s, addr):
    while True:
        try:
            data = s.recv(BUF_SIZE)
            vote = data.decode("utf-8")
            if data:
                self.votes_mutex.acquire()
                self.votes[addr] = vote
                if((len(votes) >= len(self.members)) and len(self.members)>0):
                    self.qurom = (len(self.members)/2) + 1;
                    # process votes and reset dict
                    self.process_votes()
                self.votes_mutex.release()

        except KeyboardInterrupt:
            s.close()
            return
        except Exception as e:
            return

    def process_votes():
        # majority condition check
        # reset votes
        self.votes = dict()
        # commit messages 




    def commit_messages():
        #commit message to the dict
        self.checkpoint_mutex.acquire()


        self.checkpoint_mutex.release()

    
    def client_msg_queue_proc(self):
        while True:
            # Quiescence control added here
            while self.is_in_quiescence:
                continue

            # Get job from the queue and process it
            if self.client_msg_queue.empty():
                continue
            
            # Pop a message from the queue
            if(self.current_proposal == None):
                curr_mesg = self.client_msg_queue.get()
                self.current_proposal = dict()
                self.current_proposal["type"] = "vote"
                self.current_proposal["msg"] = curr_mesg
                self.current_proposal["replica_clock"] = self.rp_msg_count
                username = curr_mesg["username"]

            # If the message has already been processed
            if curr_mesg["clock"] < self.client_proc_msg_count[username]:
                del self.client_msg_dict[(username, curr_mesg["clock"])]
                continue


            self.broadcast_votes()
        
            # TODO: Perform Lightweight gossip here, and check if you have the 
            # message in the dictionary, if not wait for it and then process it.

            
            del self.client_msg_dict[(username, data["clock"])]

            self.client_proc_msg_count[username] += 1

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

                del self.client_proc_msg_count[username]

                print(RED + "Logout from:", username + RESET)

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