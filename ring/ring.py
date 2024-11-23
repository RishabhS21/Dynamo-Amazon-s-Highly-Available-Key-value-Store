from dataclasses import dataclass
from core.logger import server_logger
from sortedcontainers import SortedSet
import hashlib
from time import sleep

from core.server import ServerInfo


def custom_comparator(server: ServerInfo):
    # Determine if it's a "Seed" or "Server"
    is_seed = server.name.startswith("Seed")
    # Extract the numeric ID after '_'
    _, id_str = server.name.split('_')
    numeric_id = int(id_str)
    # Return a tuple to define the sort order
    return (not is_seed, numeric_id)


class Ring:
    def __init__(self, serverinfo:ServerInfo, connections: list[ServerInfo], config):
        self.numserver=len(connections)+1
        self.info= serverinfo
        self.servers: SortedSet[ServerInfo] = SortedSet(connections, custom_comparator)
        self.servers.add(self.info)
        self.offline: SortedSet[ServerInfo] = SortedSet([], custom_comparator)
        self.config = config


    def encode(self) -> dict[str: str]:
        Names = ""
        Port = ""
        for info in self.servers:
            if info.name[:6]=="Server":
                serverid:str = info.name[7:]+'_'
                Names+=serverid
                port = str(info.port)+'_'
                Port+=port
        return {"type":"MEMBERSHIP", "Names": Names, "Port": Port, "from":self.info.name}
    
    def decode(self, msg: dict[str, str]) -> list[ServerInfo]:
        logger = server_logger.bind(server_name=self.info.name)

        if "Names" not in msg or "Port" not in msg:
            logger.warning("Invalid membership message: missing keys.")
            return []
        servers = []
        names = msg.get("Names").split("_")
        ports = msg.get("Port").split("_")

        if len(names) != len(ports):
            logger.error("Mismatch between names and ports in the message.")
            return servers  # Return an empty list or raise an exception

        for name, port in zip(names, ports):
            if name:  # Skip empty entries
                try:
                    servers.append(ServerInfo(name=f"Server_{name}", host=self.config["host"], port=int(port)))
                except ValueError:
                    logger.error(f"Invalid port value: {port}")
        return servers

    def equal(self, info1:ServerInfo, info2:ServerInfo):
        if (info1.name == info2.name and
            info1.host == info2.host and
            info1.port == info2.port): return True
        return False

    def SH1hash(self, input_string):
        """Generate a 40-bit hash value using SHA-1"""
        # SHA-1 generates a 20-byte hash
        sha1_hash = hashlib.sha1(input_string.encode()).digest()
        
        # Combine the first 5 bytes into an integer (40 bits)
        hash_value = 0
        for i in range(5):
            hash_value = (hash_value << 8) | sha1_hash[i]
        
        return hash_value


    def find_coordinator(self, key):
        """Find the coordinator based on the key"""
        hashkey = self.SH1hash(key)
        # Find the first position greater than or equal to the hashkey
        coor_i = self.servers.bisect_left(ServerInfo("Server_"+str(hashkey), self.config["host"], 1000))
        if (coor_i<len(self.servers)): return coor_i
        return 4
                                        

    def find_preference(self, key):
        """Find the preference list based on the key"""
        coor_i = (self.find_coordinator(key)+1)%self.numserver
        perf = []
        for i in range(self.config["N"]-1):
            j = (i+coor_i)%self.numserver
            if j<4: j+=4
            perf.append(self.servers[j])
        return perf




    
    