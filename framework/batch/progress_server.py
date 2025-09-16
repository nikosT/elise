import argparse
import csv
from datetime import timedelta
import json
import os
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
import select
import socket
import sys
from time import time
from typing import Optional
from websocket import create_connection
from websockets.sync.server import serve

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from common.communication import TCPSocket, pad_message
from common.utils import define_logger

logger = define_logger()
console = Console()


def progress_server(server_ipaddr: str, 
                    server_port: int,
                    batches: Optional[int],
                    export_reports="", 
                    webui=False,
                    ws_server_ip_addr: str = "127.0.0.1",
                    ws_server_port: int = 55501):

    logger.debug("Starting the Progress Server.")

    # Create a reusable, listening socket for the progress server
    server_sock = TCPSocket(server_ipaddr, server_port).reusable().server()

    """
        batchId1
            handler = socket
            #rem_configs = N_1
            progress = list()
            timers = list()
            task_id = ..
        
        batchId2
            handler = socket
            #rem_configs = N_2
            progress = list()
            timers = list()
            task_id = ..
        ..

        batchIdM
            handler = socket
            #rem_configs = N_M
            progress = list()
            timers = list()
            task_id = ..
        
    --------------------------------------
    handler: the socket that handles the communication with the progress server, to inform the start of a batch and the termination of one
    #rem_configs: the number of remaining simulation configurations for a specific batch that is still running
    progress: list of the current progress of the simulation configurations
    timers: list of simulation id, input id, scheduler id, scheduler name, real timespan, simulated timespan, real v simulated timespan ratio
            
    """
    batch_map = dict()

    # Create progress instance
    progress = Progress(auto_refresh=False, console=console)
    progress.start()

    # List of current available open sockets
    current_sockets = [server_sock.ref]

    if webui:
        logger.debug("Progress server established connection to WebUI.")
        webui_socket = TCPSocket(ws_server_ip_addr, ws_server_port).client()
    
    while True:

        for batch_id, batch_info in batch_map.copy().items():

            # A simulation batch has finished
            if batch_info["#rem_configs"] == 0:
                # Remove the progress bar
                progress.remove_task(batch_info["task_id"])

                handler_sock: socket.socket = batch_info["handler"]
                # Remove handler socket from current open sockets
                current_sockets.remove(handler_sock)
                # Notify the handler that the connection will terminate
                handler_sock.close()

                # Header for reports
                headers = ["Simulation ID", 
                           "Input ID",
                           "Scheduler ID",
                           "Scheduler Name", 
                           "Real Time", 
                           "Simulated Time", 
                           "Time Ratio (Simulated Days / 1 real hour)"]
                
                if export_reports:
                    os.makedirs(f"{export_reports}/{batch_id}", exist_ok=True)
                    with open(f"{export_reports}/{batch_id}/time_reports.csv", "w") as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        for row in batch_info["timers"]:
                            writer.writerow(row)
                else:

                    # Print results table and remove batch from the batch_map
                    table = Table(title=f"Batch {batch_id}", title_justify="left", title_style="bold")
                    for col in headers:
                        table.add_column(col)
                    for sim_id, row in enumerate(batch_info["timers"]):
                        ext_row = [str(sim_id)] + list(row)
                        table.add_row(*ext_row)
                    console.print(table)
                    print()

                batch_map.pop(batch_id)

                # If there is a certain number of batches passed then decrease
                if batches is not None:
                    batches -= 1
            else:
                batch_progress = sum(batch_info["progress"]) / len(batch_info["progress"])
                if webui:
                    webui_socket.send(msg={"batch_id": batch_id, "progress": batch_progress})
                else:
                    progress.update(batch_info["task_id"], completed=batch_progress)
                    progress.refresh()
            

        # If this is a batch submission and all the batches have finished then exit
        if batches is not None and batches <= 0:
            progress.stop()
            del server_sock
            if webui:
                del webui_socket
            return


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

                # If the client has given new information about their progress
                else:
                    logger.debug(msg.decode())
                    try:
                        msg_dec = str(msg.decode())
                        start_pos = msg_dec.find("{")
                        end_pos = msg_dec.find("}")
                        msg_dec = msg_dec[start_pos:end_pos+1]
                        msg_dict = json.loads(msg_dec)

                        msg_type = str(msg_dict["type"])
                        batch_id = str(msg_dict["batch_id"])

                        match msg_type:
                            case "BatchStart":
                                configs_num = int(msg_dict["#configs"])
                                # Create progress task
                                task_id = progress.add_task(description=batch_id, total=100)
                                batch_map.update({
                                    batch_id: {
                                        "handler": notified_socket,
                                        "#rem_configs": configs_num,
                                        "progress": [0] * configs_num,
                                        "timers": [[]] * configs_num,
                                        "task_id": task_id
                                    }
                                })

                            case "Progress":
                                sim_idx = int(msg_dict["sim_id"])
                                progress_perc = int(msg_dict["progress_perc"])
                                if progress_perc > batch_map[batch_id]["progress"][sim_idx]:
                                    batch_map[batch_id]["progress"][sim_idx] = progress_perc

                            case "ProgressEnd":
                                sim_idx = int(msg_dict["sim_id"])
                                inp_idx = int(msg_dict["inp_id"])
                                sched_idx = int(msg_dict["sched_id"])
                                scheduler_name = msg_dict["scheduler"]
                                real_time = float(msg_dict["real_time"])
                                sim_time = float(msg_dict["sim_time"])
                                time_ratio = sim_time / (24 * real_time)

                                batch_map[batch_id]["timers"][sim_idx] = [
                                    str(inp_idx), str(sched_idx), str(scheduler_name),
                                    str(timedelta(seconds=real_time)).replace(", ", "_"),
                                    str(timedelta(seconds=sim_time)).replace(", ", "_"),
                                    str(time_ratio)

                                ]

                                batch_map[batch_id]["#rem_configs"] -= 1

                    except Exception as e:
                        logger.debug(f"Message received: {msg.decode()}")
                        logger.exception(str(e))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="progress_server", description="A server process to monitor the progress of each simulation")
    parser.add_argument("--server_ipaddr", type=str, required=True)
    parser.add_argument("--tcp_server_port", type=int, default=54321)
    parser.add_argument("--batches", default=None)
    parser.add_argument("--export_reports", default="", type=str, help="Provde a directory to export reports for each scheduler")
    parser.add_argument("--webui", default=False, action="store_true")
    parser.add_argument("--ws_server_ipaddr", type=str, default="127.0.0.1", help="The ip address of the websocket server")
    parser.add_argument("--ws_server_port", type=int, default=55501, help="The port number of the websocket server")

    args = parser.parse_args()

    host_ipaddr = args.server_ipaddr
    tcp_port = args.tcp_server_port
    batches = args.batches
    export_reports = args.export_reports
    webui = args.webui
    ws_server_ipaddr = args.ws_server_ipaddr
    ws_server_port = args.ws_server_port

    if batches is not None:
        batches = int(batches)

    progress_server("0.0.0.0", tcp_port, batches, export_reports, webui, ws_server_ipaddr, ws_server_port)
