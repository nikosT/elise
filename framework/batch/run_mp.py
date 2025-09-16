from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import freeze_support
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator
from common.communication import TCPSocket
from common.utils import define_logger
from run_utils import multiple_simulations

if __name__ == "__main__":
    
    freeze_support()


    logger = define_logger(log_ancestry=True, log_env=True)

    schematic_file_path = sys.argv[1]
    total_procs = int(sys.argv[2])
    batch_size = int(sys.argv[3])
    server_ipaddr = sys.argv[4]
    server_port = int(sys.argv[5])
    webui = bool(int(sys.argv[6]))

    batch_creator = BatchCreator(schematic_file_path, webui)
    batch_creator.create_ranks()

    logger.debug("Notify progress server for new batch of sim configs.")
    prog_sock = TCPSocket(server_ipaddr, server_port).reusable().client()
    prog_sock.send(msg={"type": "BatchStart", "batch_id": batch_creator.batch_id, "#configs": str(total_procs)}, json_fmt=True)
    # TODO: Should check if the project name == batch id is unique

    logger.debug(f"Creating a process pool of {total_procs} max workers")
    executor = ProcessPoolExecutor(max_workers=total_procs)

    multiple_simulations_partial = partial(multiple_simulations, server_ipaddr=server_ipaddr, server_port=server_port, webui=webui)

    for i in range(total_procs):
        logger.debug(f"Worker {i} gets {batch_size} number of simulation configurations")
        executor.submit(multiple_simulations_partial, batch_creator.ranks[i*batch_size:(i+1)*batch_size])

    logger.debug(f"Waiting for the processes to finish")
    executor.shutdown(wait=True)
    logger.debug(f"The processes have finished without any errors")

    # Wait for the termination/confirmation from the progress server
    prog_sock.ref.recv(1)
