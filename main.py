import random
import json
import hashlib
from core.server import ServerInfo
from core.network import ConnectionStub
import threading
from seed.seed import Seed
from server.server import Server
sock_pool_size = 8
def load_config(filename="core/config.json"):
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

def init_seeds():
    for i in range(config["NUMSEED"]):
        connections = []
        for j in range(config["NUMSEED"]):
            if j!=i: 
                info_j = ServerInfo(f"Seed_{j}", config["host"], config["SEEDPORT"]+j)
                connections.append(info_j)
        stub_i = ConnectionStub(connections=connections, sock_pool_sz=sock_pool_size)
        info_i = ServerInfo(f"Seed_{i}", config["host"], config["SEEDPORT"]+i)
        seed = Seed(info_i, stub_i, config)
        seed_thread = threading.Thread(target=seed.start(), name=f"Initializing #Seed_{i}")
        seed_thread.daemon = True
        seed_thread.start()

def init_servers():
    for i in range(config["NUMSER"]):
        connections = []
        for j in range(config["NUMSEED"]):
            info_j = ServerInfo(f"Seed_{j}", config["host"], config["SEEDPORT"]+j)
            connections.append(info_j)
        serverid = generate_pos()
        stub_i = ConnectionStub(connections=connections, sock_pool_sz=sock_pool_size)
        info_i = ServerInfo(f"Server_{serverid}", config["host"], config["SERVERPORT"]+i)
        server = Server(info_i, stub_i, config)
        server_thread = threading.Thread(target=server.start(), name=f"Initializing #Server_{serverid}")
        server_thread.daemon = True
        server_thread.start()

if __name__ == "__main__":
    init_seeds()
    init_servers()

    
