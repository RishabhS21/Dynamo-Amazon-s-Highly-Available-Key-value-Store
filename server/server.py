from __future__ import annotations

import abc
import socket
import threading
from dataclasses import dataclass
from multiprocessing import Process
from time import sleep
from typing import TYPE_CHECKING, Optional
from concurrent.futures import ThreadPoolExecutor

# if TYPE_CHECKING:
from core.network import ConnectionStub

from core.logger import server_logger
from core.message import JsonMessage
from core.socket_helpers import STATUS_CODE, recv_message

import hashlib
import json
from ring.ring import Ring 
import random

@dataclass
class ServerInfo:
  name: str
  host: str
  port: int

  def __hash__(self) -> int:
    return hash(f"{self.name} {self.host}:{self.port}")

  def __str__(self) -> str:
    return f"Name={self.name},Address={self.host}:{self.port},"

class Server(Process):
    def __init__(self, info: ServerInfo, connection_stub: ConnectionStub, config) -> None:
        super(Server, self).__init__()
        self._info: ServerInfo = info
        self._connection_stub: ConnectionStub = connection_stub #seeds
        self.config = config
        self.serverid: str = self._info.name[7:]
        self.ring: Ring = Ring(self._info, list(self._connection_stub._connections.values()))
        self.lock = threading.Lock()

    def load_config(filename="config.json"):
        with open(filename, 'r') as file:
            config = json.load(file)
        return config
    
    def generate_serverid(self):
        """Generate a random position in the ring"""
        return random.randint(0, self.config["RING_SIZE"] - 1)

    def SH1hash(input_string):
        """Generate a 40-bit hash value using SHA-1"""
        # SHA-1 generates a 20-byte hash
        sha1_hash = hashlib.sha1(input_string.encode()).digest()
        
        # Combine the first 5 bytes into an integer (40 bits)
        hash_value = 0
        for i in range(5):
            hash_value = (hash_value << 8) | sha1_hash[i]
        
        return hash_value

    def inc_server(self, serverid):
        """Insert a new server into the ring"""
        try:
            self.ring.servers.append(serverid)
            self.ring.numserver+=1
            return True
        except Exception as e:
            print(f"Exception occurred: {e}")
            return False


    def find_coordinator(self, key):
        """Find the coordinator based on the key"""
        hashkey = self.SH1hash(key)
        # Find the first position greater than or equal to the hashkey
        for pos in self.ring.servers:
            if pos >= hashkey:
                return pos
        # If no such position is found, return the first position in the ring
        return next(iter(self.ring.servers))

    def handle_client(self, client_sock: socket.socket, addr: socket.AddressInfo):
        _logger = server_logger.bind(server_name=self._info.name)
        try:
            while True:
                _logger.debug(f"Connected with {addr}")
                err_code, request = recv_message(client_sock)

                if request is None:
                    _logger.critical(f"{STATUS_CODE[err_code]}")
                    sr = JsonMessage(msg={"error_msg": STATUS_CODE[err_code], "error_code": err_code})
                else:
                    _logger.debug(f"Received message from {addr}: {request}")
                    sr = self._process_req(request)
                if sr is not None:
                    _logger.debug(f"Sending message to {addr}: {sr}")
                    client_sock.sendall(sr.serialize())
        except Exception as e:
            _logger.exception(e)
        finally:
            _logger.debug(f"Something went wrong! Closing the socket")
            client_sock.close()

    def setup_seeds(self):
        sleep(2)
        msg = JsonMessage({"type":"PING", "name":self._info.name, "port":self._info.port})
        self._connection_stub.broadcast(msg)

    def membership(self):
        _logger = server_logger.bind(server_name=self._info.name)
        while True:
            try:
                sleep(2)
                with self.lock:
                    if not self._connection_stub._connections:
                        _logger.warning("No active connections to send membership updates.")
                        continue
                    
                    msg = JsonMessage(self.ring.encode())
                    to = random.choice(list(self._connection_stub._connections.keys()))
                    sock = self._connection_stub.get_connection(to)
                    reply = sock.send(msg)
                    # _logger.info(f"Membership update sent to {to}")
                    # _logger.info(f"Membership update response from {to}: {reply}")

            except Exception as e:
                _logger.info(f"Error in membership broadcast: {e}")

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._info.host, self._info.port))
        sock.listen()

        sleep(2)    # Let all servers start listening
        self._connection_stub.initalize_connections()
        seeds_thread = threading.Thread(target=self.setup_seeds, name="seed_ping")
        seeds_thread.start()
        membership_thread = threading.Thread(target=self.membership, name="membership")
        membership_thread.start()
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.info(f"Listening on {self._info.host}:{self._info.port}")

        client_handlers: list[threading.Thread] = []
        try:
            while True:
                client_sock, addr = sock.accept()
                client_handler = threading.Thread(target=self.handle_client,
                                                args=(client_sock, addr), name=f"listen#{addr[1]}")
                client_handler.daemon = True
                client_handler.start()
                client_handlers.append(client_handler)
        finally:
            seeds_thread.join()
            membership_thread.join()
            sock.close()
            for client_handler in client_handlers:
                client_handler.join()



    def _process_req(self, msg: JsonMessage) -> Optional[JsonMessage]:
        _logger = server_logger.bind(server_name=self._info.name)
        if (msg.get("type")=="PING"):
            _logger.info(f'Ping from on [{msg.get("name")}:{msg.get("port")}]')
            name = msg.get("name")
            port = int(msg.get("port"))
            meminfo = ServerInfo(name, self.config["host"], port)

            with self.lock:
                for info in self.ring.servers:
                    if (meminfo.name == info.name and
                        meminfo.host == info.host and
                        meminfo.port == info.port):
                        return JsonMessage({"status":1})
                self.ring.servers.append(meminfo)
                self._connection_stub.addServer(info)
                _logger.info(self.ring.servers)
            return JsonMessage({"status":1})

        elif (msg.get("type")=="MEMBERSHIP"):

            _logger.info(f'msg from {msg.get("From")} to {self._info.name}')
            _logger.info(f'Memebership {msg.get("Names")}')
            _logger.info(self.ring.servers)
            return JsonMessage({"status":1})

    def __str__(self) -> str:
        return str(self._info)
    

