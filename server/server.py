import hashlib
import json
# some definitions
NUM_SERVERS = 0
SERVERID = -1  # serverid of this server
server2pos = {}  # maps server ID to their pos in ring
pos2server = {}  # maps pos to their server ID if exist
offline = set()  # stores offline servers

def load_config(filename="config.json"):
    with open(filename, 'r') as file:
        config = json.load(file)
    return config

# Load the configuration
config = load_config()

def SH1hash(input_string):
    """Generate a 40-bit hash value using SHA-1"""
    # SHA-1 generates a 20-byte hash
    sha1_hash = hashlib.sha1(input_string.encode()).digest()
    
    # Combine the first 5 bytes into an integer (40 bits)
    hash_value = 0
    for i in range(5):
        hash_value = (hash_value << 8) | sha1_hash[i]
    
    return hash_value

def inc_server(serverid, pos):
    """Insert a new server into the ring"""
    try:
        server2pos[serverid] = pos
        pos2server[pos] = serverid
        global NUM_SERVERS
        NUM_SERVERS += 1
        return True
    except Exception as e:
        print(f"Exception occurred: {e}")
        return False

def init_server(serverid, pos):
    """Initialize the server with the given serverid and position"""
    try:
        global SERVERID
        SERVERID = serverid
        server2pos[serverid] = pos
        pos2server[pos] = serverid
        global NUM_SERVERS
        NUM_SERVERS += 1
        return True
    except Exception as e:
        print(f"Exception occurred: {e}")
        return False

def find_coordinator(key):
    """Find the coordinator based on the key"""
    hashkey = SH1hash(key)
    # Find the first position greater than or equal to the hashkey
    sorted_positions = sorted(pos2server.keys())
    for pos in sorted_positions:
        if pos >= hashkey:
            return pos2server[pos]
    # If no such position is found, return the first position in the ring
    return pos2server[0]

# Example usage in the main function
if __name__ == "__main__":
    mp = {0: 10, 1: 9, 2: 8, 3: 7}
    for key, value in mp.items():
        print(f"{key} {value}")
