import argparse
import hashlib
import json
import socket
import time
import threading
import os

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

# Global variables
users = dict()
users_mutex = threading.Lock() # Lock on users dict

message_count = 0
count_mutex = threading.Lock() # Lock on message_count

def broadcast(message):
    # Inform all other clients that a new client has joined
    users_mutex.acquire()
    for _, s_client in users.items():
        s_client.send(message.encode(STR_ENCODING))
    users_mutex.release()
    return

def get_hash(message, count):
    return hashlib.sha256(message + str(count)).hexdigest()

def client_service_thread(s, addr, verbose=False):
    # Client has connected to the server
    # We expect the first packet from the client to be a JSON login packet {"type": "login", "username":<username>}
    login_data = s.recv(BUF_SIZE)
    login_data = login_data.decode(STR_ENCODING)
    login_data = json.loads(login_data)
    if (login_data["type"] != "login"): # Wrong packet type
        # Send error message
        message = dict()
        message["type"] = "error"
        message["text"] = "Malformed packet"
        message = json.dumps(message)
        s.send(message.encode(STR_ENCODING))
        return 
    if (login_data["username"] in users): # Username already in use
        # Send failed login packet to new user
        message = dict()
        message["type"] = "error"
        message["text"] = "Username taken"
        message = json.dumps(message)
        s.send(message.encode(STR_ENCODING))
        s.close()
        return     

    # Otherwise, we accept the client
    username = login_data["username"]
    # Add the client socket to the users dictionary
    users_mutex.acquire()
    users[username] = s
    users_mutex.release()

    # Send user joined message to all other users
    message = dict()
    message["type"] = "login_success"
    message["username"] = login_data["username"]
    message = json.dumps(message)
    broadcast(message)

    
    
    # Receive, process, and retransmit chat messages from the client
    while True:
        try:
            data = s.recv(BUF_SIZE)
            data = data.decode(STR_ENCODING)
            data = json.loads(data)

            # If the client is attempting to logout
            if (data["type"] == "logout"):
                # Delete the current client from the dictionary
                users_mutex.acquire()
                del users[username]
                users_mutex.release()

                message = dict()
                message["type"] = "logout_success"
                message["username"] = username
                message = json.dumps(message)
                broadcast(message)

                s.close()
                return

            # If the client sends a normal chat message
            elif (data["type"] == "send_message"):
                chat_message = data["text"]

                message = dict()
                message["type"] = "receive_message"
                message["username"] = username
                message["text"] = chat_message

                users_mutex.acquire()
                message["id"] = message_count
                message["hash"] = get_hash(chat_message, message_count)
                message_count = message_count + 1
                users_mutex.release()

                message = json.dumps(message)
                broadcast(message)
                
            # Logging
            if(verbose): print(message)

        except:
            # Log the user out forcefully
            users_mutex.acquire()
            del users[username]
            users_mutex.release()

            message = dict()
            message["type"] = "logout_success"
            message["username"] = username
            message = json.dumps(message)
            broadcast(message)

            s.close()

    return

def tcp_server(port, verbose=False):
    host_ip = socket.gethostbyname(socket.gethostname())
    print(RED + "Starting chat server on " + str(host_ip) + ":" + str(port) + RESET)

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
    s.bind((host_ip, port))
    s.listen(5)

    try:
        while(True):
            # Accept a new connection
            conn, addr = s.accept()
            # Initiate a client listening thread
            threading.Thread(target=client_service_thread, args=(conn, addr, logfile, verbose)).start()

    except KeyboardInterrupt:
        s.close()
        print(RED + "Closing chat server on " + str(host_ip) + ":" + str(port) + RESET)
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
    tcp_server(args.port, args.verbose)

    print("\nTotal time taken: " + str(time.time() - start_time) + " seconds")

    # Exit
    os._exit(1)