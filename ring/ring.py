from dataclasses import dataclass

@dataclass
class ServerInfo:
  name: str
  host: str
  port: int


class Ring:
    def __init__(self, serverinfo:ServerInfo, connections: list[ServerInfo]):
        self.numserver=len(connections)+1
        self.info= serverinfo
        self.servers: list[ServerInfo] = connections
        self.offline: list[ServerInfo] = []

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


    
    