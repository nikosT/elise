import argparse
import csv
from datetime import timedelta
import json
import os
import select
import socket
import sys
import tabulate
from time import time
import threading
from websocket import create_connection
from websockets.sync.server import serve

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common.communication import TCPSocket, pad_message
from common.utils import define_logger

logger = define_logger()


def progress_server(server_ipaddr: str, server_port: int, clients: int, export_reports="", webui=False):

    logger.debug("Starting the Progress Server.")

    # Create a reusable, listening socket for the progress server
    server_sock = TCPSocket(server_ipaddr, server_port).reusable().server()

    # List of current available open sockets
    current_sockets = [server_sock.ref]

    # Remaining clients
    rem_clients = clients

    # Progress for all clients
    progress_list = [0] * clients
    
    # Time reports list of tuples(id, scheduler name, real time, simulated time, time ratio)
    time_reports_list: list[tuple[int, str, str, str, str]] = list()
    
    if webui:
        logger.debug("Progress server established connection to WebUI.")
        webui_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        webui_socket.connect(("127.0.0.1", 55501))
    
    while True:

        # Average progress for all clients (= simulation runs)
        overall_progress = sum(progress_list) / clients if clients > 0 else 0.0
        
        if webui:
            webui_socket.send(pad_message(str(overall_progress).encode("utf-8")))
        else:
            # Stdout print of overall progress
            print(f"\rOverall Progress: {overall_progress:.2f}%", end="")

        # If the remaining clients is zero and the only socket left is the
        # server then shutdown the progress server
        if rem_clients <= 0 and len(current_sockets) == 1 and current_sockets[0] == server_sock.ref:
            break

        # Select/poll from current_sockets
        read_sockets, _, _ = select.select(current_sockets, [], [])

        for notified_socket in read_sockets:

            # If a new connection arrives
            if notified_socket == server_sock.ref:

                client_sock, client_ipaddr = server_sock.ref.accept()
                current_sockets.append(client_sock)

            # A message arrived from a client socket
            else:

                msg = notified_socket.recv(1024)

                # The client has finished execution and exited
                if not msg:
                    current_sockets.remove(notified_socket)
                    notified_socket.close()
                    # Decrease the amount of remaining socket clients to be fullfilled
                    rem_clients -= 1

                # If the client has given new information about their progress
                else:
                    logger.debug(msg.decode())
                    try:
                        msg_dec = str(msg.decode())
                        start_pos = msg_dec.find("{")
                        end_pos = msg_dec.find("}")
                        msg_dec = msg_dec[start_pos:end_pos+1]
                        msg_dict = json.loads(msg_dec)
                        
                        sim_idx = int(msg_dict["sim_id"])

                        # Check whether it is a progress report or a time report
                        if "progress_perc" in msg_dict:

                            progress_perc = int(msg_dict["progress_perc"])
                            # Update the progress report for the specific simulation run
                            if progress_perc > progress_list[sim_idx]:
                                progress_list[sim_idx] = progress_perc

                        elif "real_time" in msg_dict:
                            inp_idx = int(msg_dict["inp_id"])
                            sched_idx = int(msg_dict["sched_id"])
                            scheduler_name = msg_dict["scheduler"]
                            real_time = float(msg_dict["real_time"])
                            sim_time = float(msg_dict["sim_time"])
                            time_ratio = sim_time / (24 * real_time)

                            time_reports_list.append((
                                sim_idx,
                                inp_idx,
                                sched_idx,
                                scheduler_name,
                                str(timedelta(seconds=real_time)).replace(", ", "_"),
                                str(timedelta(seconds=sim_time)).replace(", ", "_"),
                                str(time_ratio)
                            ))

                    except:
                        print(msg.decode())
                        pass
    
    # Sort time reports based on the simulation run ID
    time_reports_list.sort(key=lambda elem: elem[0])

    # Before closing the server print the time reports of all the simulation runs
    headers = ["Simulation ID", 
               "Input ID",
               "Scheduler ID",
               "Scheduler Name", 
               "Real Time", 
               "Simulated Time", 
               "Time Ratio (Simulated Days / 1 real hour)"]
    if export_reports:
        os.makedirs(export_reports, exist_ok=True)
        with open(f"{export_reports}/time_reports.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in time_reports_list:
                writer.writerow(row)
    else:
        print()
        print(tabulate.tabulate(time_reports_list, headers=headers, tablefmt="fancy_grid"))
    
    # Close websocket client
    if webui:
        webui_socket.close()

    # Close the server socket
    del server_sock


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="progress_server", description="A server process to monitor the progress of each simulation")
    parser.add_argument("--server_ipaddr", type=str, required=True)
    parser.add_argument("--tcp_server_port", type=int, default=54321)
    parser.add_argument("--clients", type=int, required=True)
    parser.add_argument("--export_reports", default="", type=str, help="Provde a directory to export reports for each scheduler")
    parser.add_argument("--webui", default=False, action="store_true")

    args = parser.parse_args()

    host_ipaddr = args.server_ipaddr
    tcp_port = args.tcp_server_port
    clients = args.clients
    export_reports = args.export_reports
    webui = args.webui

    progress_server(host_ipaddr, tcp_port, clients, export_reports, webui)
