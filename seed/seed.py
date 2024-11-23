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
import random
from ring.ring import Ring

@dataclass
class ServerInfo:
  name: str
  host: str
  port: int

  def __hash__(self) -> int:
    return hash(f"{self.name} {self.host}:{self.port}")

  def __str__(self) -> str:
    return f"Name={self.name},Address={self.host}:{self.port},"


class Seed(Process):
  """This class represents the Seed Server of Dynamo"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub, config) -> None:
    super(Seed, self).__init__()
    self._info = info
    self.config = config
    self._connection_stub = connection_stub # other seeds
    self.ring: Ring = Ring(self._info, list(self._connection_stub._connections.values()), self.config)
    self.lock = threading.Lock() 

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
    with self.lock:
      msg = JsonMessage({"type":"PING", "name":self._info.name, "port":self._info.port})
      self._connection_stub.broadcast(msg)

  def membership(self):
    sleep(2)
    _logger = server_logger.bind(server_name=self._info.name)
    while True:
        try:
            sleep(random.choice([1,2,3]))
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
        if self.ring.equal(self._info, meminfo): 
          return JsonMessage({"status":1})
        for info in self.ring.servers:
          if self.ring.equal(meminfo, info):
            return JsonMessage({"status":1})
        self.ring.servers.append(meminfo)
        try:
          self._connection_stub.addServer(meminfo)
        except Exception as e:
          _logger.info(f"Error in addServer: {e}")

      # _logger.info(self.ring.servers)
      _logger.info(len(self.ring.servers))

      return JsonMessage({"status":1})

    elif (msg.get("type")=="MEMBERSHIP"):
      _logger.info(f'Memebership msg from {msg.get("From")} to {self._info.name}: {msg.get("Names")}')
      servers = self.ring.decode(msg)
      _logger.info(servers)
      with self.lock:
        for meminfo in servers:
          exist = False  
          if self.ring.equal(self._info, meminfo): 
            exist = True
          if not exist:
            for info in self.ring.servers:
              if self.ring.equal(meminfo, info):
                  exist = True
                  break
        if not exist:
          self.ring.servers.append(meminfo)
          try:
            self._connection_stub.addServer(meminfo)
          except Exception as e:
              _logger.info(f"Error in addServer: {e}")
      # _logger.info(self.ring.servers)
      _logger.info(len(self.ring.servers))

      return JsonMessage({"status":1})

  def __str__(self) -> str:
    return str(self._info)
  