import os
import sys
import time
import json
import socket
import hashlib
import argparse
import threading
import multiprocessing
from collections import defaultdict

BUF_SIZE = 1024

BLACK = "\u001b[30m"
RED = "\u001b[31m"
GREEN = "\u001b[32m"
YELLOW = "\u001b[33m"
BLUE = "\u001b[34m"
MAGENTA = "\u001b[35m"
CYAN = "\u001b[36m"
WHITE = "\u001b[37m"
RESET = "\u001b[0m"


class Replica():

    def __init__(self, verbose=True, heartbeat_frequency=1):
        self.set_host_ip()
        self.ip = self.host_ip
        self.verbose = verbose
        self.client_port  = 5000
        self.HB_port      = 10000
        self.RM_port      = 15000
        self.replica_port = 20000

        # Queues and Dicts
        self.replica_processed_msg_count = 0  # Previously referred to as "rp_msg_count"
        self.client_msg_queue = multiprocessing.Queue()
        self.manager = multiprocessing.Manager()
        self.client_msg_dict = self.manager.dict()
        self.client_processed_msg_count = {}

        # Consensus Variables
        self.is_in_quiescence = True
        self.votes = dict()
        self.current_proposal = None
        self.message_to_commit = None
        self.commit_flag = False

        # Flag to indicate if checkpointing was done
        self.ckpt_received = False

        # Global variables
        self.users = dict()
        self.users_mutex = threading.Lock()  # Lock on users dict

        self.msg_count = 0
        self.count_mutex = threading.Lock()  # Lock on message_count

        self.members = dict()
        self.members_mutex = threading.Lock()  # Lock on replica members dict
        self.checkpoint_mutex = threading.Lock()  # Lock on the checkpoint creation
        self.quiescence_lock = threading.Lock()
        self.votes_mutex = threading.Lock()  # Lock on votes

        # Start the heartbeat thread
        self.start_heartbeat(interval=heartbeat_frequency)

        # Start the RM thread
        # Upon startup, this Replica will receive the add_replicas packet with its own IP.
        # It will initiate connect_to_existing_replicas() to get up to date with the other replicas.
        print(MAGENTA + 'Initialized Replica in Quiescence')
        threading.Thread(target=self.rm_thread, daemon=True).start()

        # Start the chat server
        # threading.Thread(target=self.print_membership_thread,args=(1,)).start()
        print(RED + "Starting chat server on " + str(self.host_ip) + ":" + str(self.client_port) + RESET)
        threading.Thread(target=self.client_msg_processing_queue, daemon=True).start()
        self.chat_server()

    def set_host_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

    ###############################################
    # Heartbeat functions
    ###############################################

    def start_heartbeat(self, interval):
        """
        Start heartbeating to LFD through the self.HB_port.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.connect((self.ip, self.HB_port))
            print(RED + "Connected to local fault detector at: " + self.ip + ":" + str(self.HB_port) + RESET)

        except Exception as e:
            print(e)
            return

        threading.Thread(target=self.heartbeat_thread,args=(s, interval), daemon=True).start()

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

    ###############################################
    # Replica Membership functions
    ###############################################

    def print_membership_thread(self, interval):
        while True:
            members = [addr for addr in self.members] + [self.ip]
            print("Current Membership:" +str(members))
            time.sleep(interval)

    def missing_connections(self):
        for addr in self.members:
            if (self.members[addr] is None):
                return True
        return False

    def rm_thread(self):
        # Using port 15000 for RM
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # IPv4, UDP
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.RM_port))

        except Exception as e:
            print(e)
            os.close(1)

        while True:
            try:
                data, _ = s.recvfrom(BUF_SIZE)
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

                    if self.ip in data["ip_list"]:
                        # If connected as new member --> get states from existing replicas.
                        self.members_mutex.acquire()
                        del self.members[self.ip]
                        self.members_mutex.release()
                        self.connect_to_existing_replicas()

                    else:
                        # If an exisiting member --> connect to new members.
                        time.sleep(1)
                        self.connect_to_new_replicas()
                        # data = MAGENTA + replica_ckpt['replica_processed_msg_count'] + replica_ckpt['client_processed_msg_count'] + self.ip + RESET

                elif (data["type"] == "del_replicas"):
                    self.members_mutex.acquire()
                    for replica_ip in data["ip_list"]:
                        if replica_ip not in self.members:
                            print(RED + "Received del_replicas ip that was not in membership set" + RESET)
                        else:
                            if (self.members[replica_ip] is not None):
                                self.members[replica_ip].close() # close the socket to the failed replica
                            del self.members[replica_ip]
                    self.members_mutex.release()

                else:
                    print(RED + "Received bad packet type from RM" + RESET)

                # Print out the new membership set
                members = [addr for addr in self.members] + [self.ip]
                print(RED + "Membership Updated: " + str(members) + RESET)

            except KeyboardInterrupt:
                s.close()
                return

    def create_replica_checkpoint(self):
        """
        Returns a json dict.
        """
        replica_ckpt = {}
        replica_ckpt["type"] = "checkpoint"
        replica_ckpt["replica_processed_msg_count"] = self.replica_processed_msg_count
        replica_ckpt["client_processed_msg_count"] = self.client_processed_msg_count

        replica_ckpt = json.dumps(replica_ckpt)
        return replica_ckpt

    def connect_to_existing_replicas(self):
        """
        As a newly joined replica, it needs to recieve checkpoint from the
        exisiting replicas and must also wait for the other newly joined
        replicas to do the same.
        """

        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)  # IPv4, TCPIP
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
                        if self.ckpt_received is False:

                            replica_ckpt = json.loads(data.decode("utf-8"))
                            assert(replica_ckpt["type"] == "checkpoint")
                            print(MAGENTA + 'Checkpoint received from {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)

                            self.replica_processed_msg_count = replica_ckpt["replica_processed_msg_count"]
                            self.client_processed_msg_count = replica_ckpt["client_processed_msg_count"]
                            self.ckpt_received = True
                        else:
                            assert(replica_ckpt["type"] == "checkpoint")
                            checkpoint_msg = {}
                            checkpoint_msg["type"] = "checkpoint"
                            checkpoint_msg["replica_processed_msg_count"] = self.replica_processed_msg_count
                            checkpoint_msg["client_processed_msg_count"] = self.client_processed_msg_count
                            print(MAGENTA + "Internal State: {}".format(checkpoint_msg) + RESET)
                            print(MAGENTA + "Checkpoint {}: {}".format(addr, checkpoint_msg) + RESET)

                except KeyboardInterrupt:
                    s.close()
                    return

                print(RED + "Received connection from existing replica at" + addr + ":" + str(self.replica_port) + RESET)
                # threading.Thread(target=self.replica_send_thread,args=(conn,), daemon=True).start()
                threading.Thread(target=self.replica_to_replica_receive_thread, args=(conn,addr), daemon=True).start()

            self.is_in_quiescence = False
            print(MAGENTA + "Quiescence ended" + RESET)
            self.members_mutex.release()

        except KeyboardInterrupt:
            s.close()
            return

        except Exception as e:
            s.close()
            print(e)

    def connect_to_new_replicas(self):
        """
        As a replica already part of the network, it needs to
        connect to the new replicas and send them a checkpoint.
        """
        self.quiescence_lock.acquire()
        self.is_in_quiescence = True
        print(MAGENTA + "Quiescence started: Connecting to new replicas" + RESET)

        for addr in self.members:
            if self.members[addr] is None:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.connect((addr, self.replica_port))
                    self.members_mutex.acquire()
                    self.members[addr] = s
                    self.members_mutex.release()
                    print(RED + "Connected to new replica at: " + addr + ":" + str(self.replica_port) + RESET)

                    # checkpointing
                    self.checkpoint_mutex.acquire()
                    replica_ckpt = self.create_replica_checkpoint()
                    self.checkpoint_mutex.release()
                    try:
                        s.send(replica_ckpt.encode("utf-8"))
                        print(MAGENTA + 'Checkpoint sent to {}: {}'.format(addr, replica_ckpt) + self.ip + RESET)
                    except:
                        print(RED + 'Failed while sending a Replica checkpoint to:' + self.members[addr] + RESET)

                    threading.Thread(target=self.replica_to_replica_receive_thread, args=(s, addr)).start()

                except KeyboardInterrupt:
                    s.close()
                    self.quiescence_lock.release()
                    return

                except Exception as e:
                    print(e)
                    s.close()
                    self.quiescence_lock.release()
                    

        self.is_in_quiescence = False
        self.quiescence_lock.release()
        print(MAGENTA + "Quiescence ended: Connected to all new replicas." + RESET)

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
    def get_hash(self, message, count):
        return hashlib.sha256(message + str(count)).hexdigest()

    def client_service_thread(self, s, addr):
        """
        Handles the initialization of a new client and starts
        queuing the queries from the client.
        Logout protocols are handled in client_msg_processing_queue.
        """

        # When client first connects to server --> We expect the first packet
        # to be a JSON login packet {"type": "login", "username":<username>, "clock":0}.
        login_data = s.recv(BUF_SIZE)
        login_data = login_data.decode("utf-8")
        login_data = json.loads(login_data)

        if (login_data["type"] != "login"):  # Wrong packet type
            # Send error message
            message = dict()
            message["type"] = "error"
            message["text"] = "Malformed packet"
            message = json.dumps(message)
            s.send(message.encode("utf-8"))
            s.close()
            return

        if (login_data["username"] in self.users):  # Username already in use
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

        if username not in self.client_processed_msg_count:
            self.client_processed_msg_count[username] = 0

        # Insert job in client queue
        self.client_msg_dict[(username, login_data["clock"])] = login_data
        self.client_msg_queue.put(login_data)

        # Receive, process, and retransmit chat messages from this client
        while True:
            try:
                data = s.recv(BUF_SIZE)
                data = data.decode("utf-8")
                data = json.loads(data)

                self.client_msg_dict[(username, data["clock"])] = data
                self.client_msg_queue.put(data)

            except:
                print(RED + "{} has disconnected without logging out!".format(username) + RESET)
                s.close()
                return
        return

    ###############################################
    # Consensus Functionalities
    ###############################################

    def broadcast_msg(self, message):
        # param message: dict
        # sends message to all clients
        self.users_mutex.acquire()
        for _, s_client in self.users.items():
            s_client.send(json.dumps(message).encode("utf-8"))
        self.users_mutex.release()
        return

    def broadcast_votes(self):
        # send proposals to all other replicas.
        self.members_mutex.acquire()
        for addr in self.members:
            if self.members[addr] is not None:
                try:
                    self.members[addr].send(json.dumps(self.current_proposal).encode('utf-8'))
                except Exception:
                    print('Exception while boardcasting')
                    self.members[addr].close()
                    continue
        self.members_mutex.release()

    def replica_to_replica_receive_thread(self, s, addr):
        """
        TODO: How do we prevent a vote from a given replica at 't' from
              overwriting the vote at 't-1'?
        """
        try:
            while True:
                if self.is_in_quiescence:
                    continue

                try:
                    connection = self.members[addr]

                    # connection.settimeout(10)
                    data = s.recv(BUF_SIZE)

                    if data:
                        data = json.loads(data.decode("utf-8"))
                        if data['type'] == 'vote':
                            #print('Received Vote from:', addr)
                            self.votes_mutex.acquire()
                            self.votes[addr] = data['client_msg']

                            if (len(self.votes) >= len(self.members)):
                                print(self.votes)
                                self.process_votes()
                                self.commit_flag = True

                            self.votes_mutex.release()
                        else:
                            print('Non-vote data recieved: ', data)

                except Exception as e:
                    print(e)
                    time.sleep(1)  # Random Hack: Hoping to sync with RM membership updates
                    # self.votes_mutex.acquire()
                    # if(len(self.votes) >= len(self.members)):
                    #     self.process_votes()
                    #     self.commit_flag = True
                    self.votes_mutex.release()
                    s.close()
                    return

        except KeyboardInterrupt:
            s.close()
            return

        except Exception as e:
            return

    def process_votes(self):
        """
        Collect proposals from all the replicas on which client message to process
        and then set that message to self.message_to_commit.

        This processing of votes happens individually in each replica.

        TODO:
            1. Need to account for the case where, all the votes from other replicas
               are recieved and in turn invoke process_votes subroutine as usual. However,
               assume that the self.current_proposal is not yet ready for that round
               which is prepared in self.client_msg_processing_queue thread.

               What happens now? In the current implementation, we are going to use the
               old stale vote from the previous round and use it for consensus.
        """
        #print('Processing votes')
        vote_to_commit = None
        count_votes = defaultdict(lambda: 0)

        self.members_mutex.acquire()
        quorum = (len(self.members)//2) + 1
        self.members_mutex.release()
        #print(quorum)

        while(self.current_proposal is None):
            pass

        #print("done waiting for current proposal")

        # Collect votes
        # First consider the replicas, self.current_proposal
        #print(self.current_proposal)
        if self.current_proposal["client_msg"]['clock'] == self.client_processed_msg_count[self.current_proposal["client_msg"]["username"]]:
            count_votes[(self.current_proposal["client_msg"]['username'], self.current_proposal["client_msg"]['clock'])] += 1
        else:
            # Current hypothesis is that TCP should ensure this condition should never happen.
            # If it does, call Ashwin.
            raise Exception('Recieved a client message with clock (t+k) ahead of (t)!')

        #print(self.votes)
        # Now, collect the proposals from rest of the replicas.
        for vote in self.votes.values():
            if vote['clock'] == self.client_processed_msg_count[vote['username']]:
                count_votes[(vote['username'], vote['clock'])] += 1
            else:
                raise Exception('Recieved a client message with clock (t+k) ahead of (t)!')

        #print('count_votes:', count_votes)
        # Check for majority vote:
        for key in count_votes.keys():
            if (count_votes[key] >= quorum):
                vote_to_commit = key

        if (vote_to_commit is None):
            # Vote Logic added by Ashwin. Reach out if you need clarity.
            min_vote_clock = sorted(votes.keys(), key=lambda ele: ele[1])[0][1]
            vote_to_commit = sorted(votes.keys(), key=lambda ele: 'zzzzzz' if ele[1]>min_vote_clock else ele[0])[0]
            print(YELLOW + 'Consensus reached by picking based on alphabetical order of client name with lowest clock!' + RESET)
        else:
            pass
            #print('Consensus Reached by Majority')

        for vote in self.votes.values():
            if vote['username'] == vote_to_commit[0] and vote['clock'] == vote_to_commit[1]:
                self.message_to_commit = vote
                break

        #print('vote to commit', vote_to_commit)

        print(GREEN + 'message to commit:', str(self.message_to_commit) + RESET)

        # Reset votes
        self.votes = dict()
        return

    def client_msg_processing_queue(self):
        """
        Client Messages Processing Queue:
            1: The replica pops a message from the msg queue as its current
            proposal and broadcasts it to all other replicas.

            2: Once consensus is reached on which message to vote, the self.commit_flag
            is set to True. Then we commit that message.

            3: If the Replicas current proposal was the message that was committed, then
            go to step 1 else broadcast the same current proposal in the next round too.
        """
        while True:
            self.quiescence_lock.acquire()
            while self.is_in_quiescence:
                # We dont process any messages.
                pass

            # Get job from the queue and process it
            if self.client_msg_queue.empty():
                self.quiescence_lock.release()
                continue

            # Pop a message from the queue
            if(self.current_proposal is None):
                current_msg = self.client_msg_queue.get()
                # current_msg --> {"type": "login/logout/send_message", "username":<username>, "clock":0}.

                # If the message has already been processed
                if current_msg["clock"] < self.client_processed_msg_count[current_msg['username']]:
                    print("Discarded a previously processed message from:", current_msg['username'])
                    del self.client_msg_dict[(current_msg['username'], current_msg["clock"])]
                    self.quiescence_lock.release()
                    continue

                self.current_proposal = dict()
                self.current_proposal["type"] = "vote"
                self.current_proposal["client_msg"] = current_msg

            self.broadcast_votes()

            # No other replicas
            if (len(self.members) == 0):
                #print('Consensus Reached')  
                self.message_to_commit = current_msg
                print(GREEN + 'message to commit:', str(self.message_to_commit) + RESET)
                self.commit_flag = True

            while(self.commit_flag is False):
                pass

            #########################################################
            ### Broadcast message to be committed to all clients. ###
            #########################################################

            broadcast_message_to_clients = dict()
            username = self.message_to_commit["username"]

            # Login Packet
            if (self.message_to_commit["type"] == "login"):
                # Send user joined message to all other users
                broadcast_message_to_clients["type"] = "login_success"
                broadcast_message_to_clients["username"] = username
                broadcast_message_to_clients["text"] = ''
                broadcast_message_to_clients["replica_clock"] = self.replica_processed_msg_count

            # If the client is attempting to logout
            elif (self.message_to_commit["type"] == "logout"):
                s = self.users[username]
                # Delete the current client from the dictionary
                self.users_mutex.acquire()
                del self.users[username]
                self.users_mutex.release()
                self.client_processed_msg_count[username] += 1
                del self.client_processed_msg_count[username]
                # TODO: Stop the client service thread for this particular client!

                print(RED + "Logout from:", username + RESET)

                broadcast_message_to_clients["type"] = "logout_success"
                broadcast_message_to_clients["username"] = username
                broadcast_message_to_clients["text"] = ''
                broadcast_message_to_clients["replica_clock"] = self.replica_processed_msg_count
                self.broadcast_msg(broadcast_message_to_clients)
                s.close()

            # If the client sends a normal chat message
            elif (self.message_to_commit["type"] == "send_message"):
                broadcast_message_to_clients["type"] = "receive_message"
                broadcast_message_to_clients["username"] = username
                broadcast_message_to_clients["text"] = self.message_to_commit['text']
                broadcast_message_to_clients["replica_clock"] = self.replica_processed_msg_count

            # Broadcast to all clients
            self.broadcast_msg(broadcast_message_to_clients)

            # After the client message is processed and commited.
            self.commit_flag = False
            self.replica_processed_msg_count += 1
            if broadcast_message_to_clients["type"] != "logout_success":
                self.client_processed_msg_count[username] += 1

            # Retain current proposal if it was not chosen by majority
            # and propose the same proposal in the next round.
            if self.message_to_commit == self.current_proposal["client_msg"]:
                self.current_proposal = None

            #del self.client_msg_dict[(username, self.message_to_commit["clock"])]
            # print(YELLOW + "(PROC) -> {}".format(current_msg) + RESET)
            self.quiescence_lock.release()

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
    parser.add_argument('-hbf', '--hb_freq', help="Heartbeat Frequency", type=int, default=1)

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()

    args = get_args()
    replica_obj = Replica(args.verbose, args.hb_freq)
    print("\nTotal time taken: " + str(time.time() - start_time) + " seconds")

    # Exit
    sys.exit(1)