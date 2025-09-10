import argparse
import os
import sys
from time import time, sleep
import threading
from websocket import create_connection
from websockets.sync.server import serve


sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))
from common.utils import define_logger
from common.communication import pad_message, get_ip, TCPSocket

logger = define_logger()

ws_ipaddr = get_ip()
ws_port = 55500
ws_clients = set()


def start_tcp_server(host="127.0.0.1", port=55501):
    
    # Setup websocket connection
    START_TIME = time()
    UPDATE_TIME = 3
    ws_client = create_connection(f"ws://{ws_ipaddr}:{ws_port}")

    # Create a TCP/IP socket
    server_socket = TCPSocket(host, port).reusable().server()

    logger.debug(f'WebUI tcp server listening on {host}:{port}.')

    while True:
        # Wait for a connection
        client_socket, client_address = server_socket.ref.accept()
        logger.debug(f'Connection from {client_address}')

        try:
            while True:
                # Receive data from the client
                data = client_socket.recv(1024)
                if not data:
                    break  # No more data, exit the loop
                if time() - START_TIME > UPDATE_TIME:
                    ws_client.send(data)
                    START_TIME = time()
                
        finally:
            # Clean up the connection
            client_socket.close()
            logger.debug(f'Connection with {client_address} closed.')
            sleep(UPDATE_TIME)
            ws_client.send(pad_message("100.0".encode("utf-8")))

def _handler(wsocket):
    global ws_clients

    if wsocket not in ws_clients:
        ws_clients.add(wsocket)
        logger.debug(f"New WebUI client with websocket: {wsocket}")
    
    # Broadcast message from server to all the clients
    for msg in wsocket:
        for ws_client in ws_clients.copy():
            try:
                ws_client.send(msg, text=True)
            except:
                # Remove client if it is not responding
                ws_clients.pop(ws_client)



def websocket_server():
    with serve(_handler, ws_ipaddr, ws_port, ping_timeout=None) as wsocket:
        wsocket.serve_forever()

def main():
    t1 = threading.Thread(target=start_tcp_server)
    t2 = threading.Thread(target=websocket_server)
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()

if __name__ == "__main__":
    main()