from dataclasses import dataclass
from core.logger import server_logger

@dataclass
class ServerInfo:
  name: str
  host: str
  port: int


class Ring:
    def __init__(self, serverinfo:ServerInfo, connections: list[ServerInfo], config):
        self.numserver=len(connections)+1
        self.info= serverinfo
        self.servers: list[ServerInfo] = connections
        self.offline: list[ServerInfo] = []
        self.config = config

    def encode(self) -> dict[str: str]:
        Names:str = self.info.name[7:]+'_' if self.info.name[:6]=="Server" else ""
        Port:str = str(self.info.port)+'_' if self.info.name[:6]=="Server" else ""
        for info in self.servers:
            if info.name[:6]=="Server":
                serverid:str = info.name[7:]+'_'
                Names+=serverid
                port = str(info.port)+'_'
                Port+=port
        return {"type":"MEMBERSHIP", "Names": Names, "Port": Port, "From":self.info.name}
    
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

    
    