import random
import json
import hashlib
from core.server import ServerInfo
from core.network import ConnectionStub

NUM_SERVERS=0
connectionstub = ConnectionStub(set(), 8)
PORT:dict[int, int] = {}

def load_config(filename="config.json"):
    with open(filename, 'r') as file:
        config = json.load(file)
    return config

# Load the configuration
config = load_config()

def generate_pos():
    """Generate a random position in the ring"""
    return random.randint(0, config["RING_SIZE"] - 1)

def SH1hash(input_string):
    """Generate a 40-bit hash value using SHA-1"""
    # SHA-1 generates a 20-byte hash
    sha1_hash = hashlib.sha1(input_string.encode()).digest()
    
    # Combine the first 5 bytes into an integer (40 bits)
    hash_value = 0
    for i in range(5):
        hash_value = (hash_value << 8) | sha1_hash[i]
    
    return hash_value

def init_server():
    global NUM_SERVERS
    serverid = NUM_SERVERS+1
    pos = generate_pos()
    port = len(PORT) + config["SPORT"]
    serverinfo = ServerInfo("server_{}".format(serverid), config["host"], port)
    connectionstub.addServer(serverinfo)
    PORT

    
