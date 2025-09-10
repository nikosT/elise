from abc import ABC, abstractmethod
import json
import socket
from typing import Any
from websocket import create_connection
from websockets.sync.server import serve


def pad_message(msg):
    DEFAULT_MSG_LEN = 1024
    return msg + b'\0' * (DEFAULT_MSG_LEN- len(msg))


def get_ip():
    """Get the IP address of the head machine"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


class Socket(ABC):

    def __init__(self, ip_addr: str, port: int):
        self._ip_addr = ip_addr
        self._port = port
    
    @abstractmethod
    def client(self, *args, **kwargs) -> Any:
        raise NotImplemented("This should be implemented.")
    
    @abstractmethod
    def server(self, *args, **kwargs) -> Any:
        raise NotImplemented("This should be implemented.")
    
    @abstractmethod
    def send(self, *args, **kwargs) -> Any:
        raise NotImplemented("This should be implemented.")


class TCPSocket(Socket):

    def __init__(self, ip_addr: str, port: int):
        Socket.__init__(self, ip_addr, port)
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def __del__(self):
        try:
            self.__socket.shutdown()
        except:
            pass

    @property
    def ref(self):
        return self.__socket

    def reusable(self):
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        return self
    
    def nonblocking(self):
        self.__socket.setblocking(False)
        return self
    
    def client(self):
        self.__socket.connect((self._ip_addr, self._port))
        return self
    
    def server(self, connections = 1):
        self.__socket.bind((self._ip_addr, self._port))
        self.__socket.listen(connections)
        return self
    
    def send(self, 
             msg: Any, 
             json_fmt: bool = False, 
             pad_msg: bool = True, 
             close_on_sent: bool = False, reconnect_on_failure: bool = False):

        if json_fmt:
            msg = json.dumps(msg)
        
        # Transform message to bytes if not already
        msg = str(msg)
        msg = msg.encode()

        if pad_msg:
            msg = pad_message(msg)

        try:
            self.__socket.send(msg)
        except:
            #TODO: If it fails log that it failed
            # Get necessary information about connection before closing the failing socket
            # ip_addr, port = self.__socket.getsockname()
            self.__socket.close()

            # Re-establish connection
            if reconnect_on_failure:
                self.client()
            else:
                self.__socket.close()
                # self.__socket = TCPSocket(ip_addr, port).client().nonblocking().ref
            # else:
            #     self.__socket = None
        finally:
            if close_on_sent:
                self.__socket.close()
        

class WebSocket(Socket):

    def __init__(self, ip_addr: str, port: int):
        Socket.__init__(self, ip_addr, port)
    
    def client(self):
        self.__socket = create_connection(f"ws://{self._ip_addr}:{self._port}")
        return self
    
    def server(self, handler):
        with serve(handler, host=self._ip_addr, port=self._port) as serve_socket:
            self.__socket = serve_socket.socket
            serve_socket.serve_forever()