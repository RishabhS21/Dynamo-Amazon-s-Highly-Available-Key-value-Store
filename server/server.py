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

import json
from ring.ring import Ring
import random

import psycopg2
from psycopg2 import sql

from core.server import ServerInfo

# Database connection parameters for the default database
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"      # Connect to the 'postgres' database
DB_USER = "postgres"      # Your database user
DB_PASSWORD = "postgres"  # Your database password



class Server(Process):
    def __init__(
        self, info: ServerInfo, connection_stub: ConnectionStub, config
    ) -> None:
        super(Server, self).__init__()
        self._info: ServerInfo = info
        self._connection_stub: ConnectionStub = connection_stub  # seeds
        self.config = config
        self.serverid: str = self._info.name[7:]
        self.ring: Ring = Ring(
            self._info, list(self._connection_stub._connections.values()), self.config
        )
        self.lock = threading.Lock()
        self.db: str = self._info.name
        _logger = server_logger.bind(server_name=self._info.name)

        """Setup the database for the server."""
        while True:
            try:
                # Connect to the default 'postgres' database
                self.db_connection = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                self.db_connection.autocommit = True  # Ensure changes are committed without explicit commit()

                # Create a cursor object
                self.db_cursor = self.db_connection.cursor()
                create_table_query = f"""
                    CREATE TABLE {self._info.name} (
                        key VARCHAR(255) PRIMARY KEY,
                        val TEXT,
                        ver TEXT
                    );
                """
                self.db_cursor.execute(create_table_query)
                _logger.info(f"Created table {self._info.name}")
                return
            except Exception as e:
                _logger.info(f"Error connecting to database: {e}")
                sleep(1)
    def load_config(filename="config.json"):
        with open(filename, "r") as file:
            config = json.load(file)
        return config

    def handle_client(self, client_sock: socket.socket, addr: socket.AddressInfo):
        _logger = server_logger.bind(server_name=self._info.name)
        try:
            while True:
                _logger.debug(f"Connected with {addr}")
                err_code, request = recv_message(client_sock)

                if request is None:
                    _logger.critical(f"{STATUS_CODE[err_code]}")
                    sr = JsonMessage(
                        msg={"error_msg": STATUS_CODE[err_code], "error_code": err_code}
                    )
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
        msg = JsonMessage(
            {"type": "PING", "name": self._info.name, "port": self._info.port}
        )
        with self.lock:
            self._connection_stub.broadcast(msg)

    def membership(self):
        _logger = server_logger.bind(server_name=self._info.name)
        sleep(1)
        while True:
            try:
                sleep(random.choice([1, 2, 3]))
                with self.lock:
                    msg = JsonMessage(self.ring.encode())
                    to = random.choice(list(self._connection_stub._connections.keys()))
                sock = self._connection_stub.get_connection(to)
                reply = sock.send(msg)

            except Exception as e:
                _logger.info(f"Error in membership broadcast: {e}")

    def udp_server(self):
        # Create a UDP socket for clients
        _logger = server_logger.bind(server_name=self._info.name)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_sock:
            # Bind the socket to the address and port
            udp_sock.bind((self._info.host, self._info.port))
            _logger.info(f"UDP server listening on {self._info.host}:{self._info.port}")

            while True:
                # Receive data from the client
                request, addr = udp_sock.recvfrom(1024)  # Buffer size is 1024 bytes
                request = json.loads(request.decode("utf-8"))
                request = JsonMessage(request)
                _logger.debug(f"Received message from {addr}: {request}")
                resp = self._process_req(request)
                resp = resp._msg_d
                if resp is not None:
                    _logger.debug(f"Sending message to {addr}: {resp}")
                    serialized_resp = json.dumps(resp).encode("utf-8")
                    try:
                        # Send the serialized data
                        client_socket.sendto(serialized_resp, addr)
                    except:
                        _logger.debug(f"Error in sending response to request {request}")

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._info.host, self._info.port))
        sock.listen()
        sleep(2)  # Let all servers start listening
        self._connection_stub.initalize_connections()
        seeds_thread = threading.Thread(target=self.setup_seeds, name="seed_ping")
        seeds_thread.start()
        membership_thread = threading.Thread(target=self.membership, name="membership")
        membership_thread.start()
        udpserver_thread = threading.Thread(target=self.udp_server, name="udp_server")
        udpserver_thread.start()
        
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.info(f"Listening on {self._info.host}:{self._info.port}")

        client_handlers: list[threading.Thread] = []
        try:
            while True:
                client_sock, addr = sock.accept()
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_sock, addr),
                    name=f"listen#{addr[1]}",
                )
                client_handler.daemon = True
                client_handler.start()
                client_handlers.append(client_handler)
        finally:
            seeds_thread.join()
            membership_thread.join()
            udpserver_thread.join()
            sock.close()
            for client_handler in client_handlers:
                client_handler.join()


    def forwardPref(self, msg: JsonMessage, pref: list[ServerInfo]):
        """
        Forward a message to the servers in the preference list using threads, with a timeout for each send operation.

        Args:
            msg (JsonMessage): The message to be forwarded.
            pref (list[ServerInfo]): A list of preferred servers to forward the message to.
            timeout (float): The timeout in seconds for each thread.

        Returns:
            dict: A dictionary with server names as keys and their responses or errors as values.
        """
        _logger = server_logger.bind(server_name=self._info.name)
        results = {}
        threads = []
        response_lock = threading.Lock()

        def send_message(server: ServerInfo):
            nonlocal results
            try:
                sock = self._connection_stub.get_connection(server.name)
                _logger.debug(f"Sending message {msg} to server {server}")
                response = sock.send(msg)
                if response is not None:
                    _logger.debug(f"Received response from {server}: {response}")
                    with response_lock:
                        results[server.name] = response
            except Exception as e:
                _logger.error(f"Failed to send message to {server}: {e}")

        # Create threads for each server in the preference list
        for server in pref:
            thread = threading.Thread(target=send_message, args=(server,))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        return results
    
    def put(self, key, val, ver):
        _logger = server_logger.bind(server_name=self._info.name)
        try:
            query = f"""
                INSERT INTO {self._info.name} (key, val, ver)
                VALUES (%s, %s, %s)
                ON CONFLICT (key)
                DO UPDATE SET
                    val = EXCLUDED.val,
                    ver = EXCLUDED.ver;
            """
            # Use parameterized queries to safely pass values
            self.db_cursor.execute(query, (key, val, ver))
            return True  # Success
        except Exception as e:
            # Log the exception for debugging
            _logger.info(f"Error inserting into database: {e}")
            return False  # Failure
        
    def get_version(self, key):
        verQuery = f"SELECT ver FROM {self._info.name} WHERE key = %s"
        self.db_cursor.execute(verQuery, (key,))
        if self.db_cursor.rowcount > 0:
            return self.db_cursor.fetchone()[0]  # Extract the first element of the tuple
        return None
    
    def get_val(self, key):
        verQuery = f"SELECT val FROM {self._info.name} WHERE key = %s"
        self.db_cursor.execute(verQuery, (key,))
        if self.db_cursor.rowcount > 0:
            return self.db_cursor.fetchone()
        return None

    def increment_version(self, ver):
        ver = json.loads(ver)
        ver[self._info.name] = str(int(ver[self._info.name])+1)
        ver = json.dumps(ver)
        return ver
    
    def get_all_keys(self):
        query = f"SELECT key FROM {self._info.name};"
        self.db_cursor.execute(query)
        if self.db_cursor.rowcount > 0:   
            return [row[0] for row in self.db_cursor.fetchall()]
        return []

    def if_trans(self, key: str):
        pref = self.ring.find_preference(key)
        last = pref[self.config["N"]-1]
        return self.ring.equal(last, self._info)

    def delete_row(self, key: str):
        with self.lock:  # Ensure thread-safety for database operations
            delete_query = f"DELETE FROM {self._info.name} WHERE key = %s;"
            self.db_cursor.execute(delete_query, (key,))
            self.db_connection.commit()


    def transfer_keys_to_new_server(self, new_server: ServerInfo):
        _logger = server_logger.bind(server_name=self._info.name)
        _logger.info(f"Transferring keys to new server: {new_server}")

        # Find keys that the new server is responsible for
        keys_to_transfer = []
        for key in self.get_all_keys():  # Implement a method to get all keys in this server
            if self.if_trans(key):
                keys_to_transfer.append(key)

        # Send keys and values to the new server
        for key in keys_to_transfer:
            val = self.get_val(key)
            ver = self.get_version(key)
            msg = JsonMessage({"type": "RPUT", "key": key, "val": val, "ver": ver})
            while True:
                try:
                    sock = self._connection_stub.get_connection(new_server.name)
                    response = sock.send(msg)
                    if response and response.get("status") == "1":
                        _logger.info(f"Successfully transferred key {key} to {new_server}")
                        self.delete_row(key)
                        break
                    else:
                        _logger.error(f"Failed to transfer key {key} to {new_server}")
                except Exception as e:
                    _logger.error(f"Error transferring key {key} to {new_server}: {e}")


    def _process_req(self, msg: JsonMessage) -> Optional[JsonMessage]:
        _logger = server_logger.bind(server_name=self._info.name)

        if msg.get("type") == "PING":
            _logger.info(f'Ping from on [{msg.get("name")}:{msg.get("port")}]')
            name = msg.get("name")
            port = int(msg.get("port"))
            meminfo = ServerInfo(name, self.config["host"], port)
            with self.lock:
                pos = self.ring.servers.bisect_left(meminfo)
                # _logger.info(f"{meminfo} : position: {pos} : {self.ring.servers}")
                if pos<self.ring.numserver and self.ring.equal(self.ring.servers[pos], meminfo):
                    return JsonMessage({"status": 1})
                self.ring.servers.add(meminfo)
                self.ring.numserver += 1
                try:
                    self._connection_stub.addServer(meminfo)
                except Exception as e:
                    _logger.info(f"Error in addServer: {e}")
            # _logger.info(self.ring.servers)
            # _logger.info(self.ring.numserver)
            return JsonMessage({"status": 1})

        elif msg.get("type") == "MEMBERSHIP":
            _logger.info(
                f'Memebership msg from {msg.get("from")} to {self._info.name}: {msg.get("Names")}'
            )
            servers = self.ring.decode(msg)
            new_servers = []
            with self.lock:
                for meminfo in servers:
                    exist = False
                    if not exist:
                        pos = self.ring.servers.bisect_left(meminfo)
                        if pos<self.ring.numserver and self.ring.equal(self.ring.servers[pos], meminfo):
                            exist = True
                    if not exist:
                        self.ring.servers.add(meminfo)
                        new_servers.append(meminfo)
                        self.ring.numserver += 1
                        try:
                            self._connection_stub.addServer(meminfo)
                        except Exception as e:
                            _logger.info(f"Error in addServer: {e}")
                # _logger.info(self.ring.servers)
                # _logger.info(self.ring.numserver)
                # Initiate key transfer to new servers
                for new_server in new_servers:
                    self.transfer_keys_to_new_server(new_server)

            return JsonMessage({"status": 1})
        
        elif msg.get("type") == "PUT":
            _logger.info(f"Recieved PUT request {msg}")
            key = msg.get("key")
            val = msg.get("val")
            coordinator = self.ring.servers[self.ring.find_coordinator(key)]
            if not self.ring.equal(self._info, coordinator):
                _logger.info(
                    f"Redirecting PUT request {msg} to {coordinator}"
                )
                sock = self._connection_stub.get_connection(coordinator.name)
                return sock.send(msg)
            pref = self.ring.find_preference(key)
            pref.remove(self.ring.info)

            _logger.info(f"Correct Coordinator for request: {msg} : Preference list {pref}")

            ver = self.get_version(key)

            if ver is None:
                ver = {self._info.name: "0"}
                for p in pref:
                    ver[p.name] = "0"
                ver = json.dumps(ver)
            ver = self.increment_version(ver)
            msg["type"]="RPUT"
            msg["ver"]=ver
            responses = self.forwardPref(msg, pref)
            succ = 0
            for resp in responses.values():
                if resp["status"]=="1":
                    succ+=1
            # _logger.info(f"succ len: {succ}")
            if (succ>=self.config["W"]) and self.put(key, val, ver):
                return JsonMessage({"status": "1"})
            else:
                return JsonMessage({"status": "0"})

        elif msg.get("type") == "RPUT":
            _logger.info(f"Recieved RPUT request {msg}")
            key = msg.get("key")
            val = msg.get("val")
            ver = msg.get("ver")
            
            if self.put(key, val, ver):
                return JsonMessage({"status": "1"})
            else:
                return JsonMessage({"status": "0"})
            
        elif msg.get("type") == "GET":
            _logger.info(f"Recieved GET request {msg}")
            key = msg.get("key")
            coordinator = self.ring.servers[self.ring.find_coordinator(key)]
            if not self.ring.equal(self._info, coordinator):
                _logger.info(
                    f"Redirecting GET request to {coordinator}"
                )
                sock = self._connection_stub.get_connection(coordinator.name)
                return sock.send(msg)
            pref = self.ring.find_preference(key)
            _logger.info(f"Correct Coordinator for request: {msg} : Preference list {pref}")
            msg["type"]="RGET"
            responses = self.forwardPref(msg, pref)
            succ = 0
            for resp in responses.values():
                if resp["status"]=="1":
                    succ+=1
            
            if (succ>=self.config["R"]):
                val = self.get_val(key)
                if val is not None:
                    _logger.info(f"Replying get request {msg}: key: {key}, val: {val}")
                    return JsonMessage({"status":"1", "key": key, "val": val})
                else:
                    return JsonMessage({"status": "0"})
            
        elif msg.get("type") == "RGET":
            _logger.info(f"Recieved RGET request {msg}")
            key = msg.get("key")
            val = self.get_val(key)
            if val is not None:
                return JsonMessage({"status":"1", "key": key, "val": val})
            else:
                return JsonMessage({"status": "0"})


    def __str__(self) -> str:
        return str(self._info)
