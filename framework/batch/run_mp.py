from base64 import b64decode
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import freeze_support, get_context
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import SchematicAnalysis
from common.communication import TCPSocket
from common.utils import define_logger
from run_utils import multiple_simulations

if __name__ == "__main__":
    
    freeze_support()


    logger = define_logger(log_ancestry=True, log_env=True)

    schematic_file_path = sys.argv[1]
    total_procs = int(sys.argv[2])
    branges = str(sys.argv[3])
    server_ipaddr = sys.argv[4]
    server_port = int(sys.argv[5])
    export_reports = str(sys.argv[6])
    webui = bool(int(sys.argv[7]))

    branges_dec = b64decode(branges.encode())
    branges_str = branges_dec.decode()
    batch_ranges = list(map(lambda x: int(x), branges_str.split()))

    schematic_analysis = SchematicAnalysis(schematic_file_path, webui)
    batch = schematic_analysis.batch

    logger.debug("Notify progress server for new batch of sim configs.")
    prog_sock = TCPSocket(server_ipaddr, server_port).client()
    prog_sock.send(msg={"type": "BatchStart", "batch_id": schematic_analysis.batch_id, "#configs": sum(batch_ranges), "export_reports": export_reports}, json_fmt=True)

    logger.debug(f"Creating a process pool of {total_procs} max workers")
    executor = ProcessPoolExecutor(max_workers=total_procs, mp_context=get_context("spawn"))

    multiple_simulations_partial = partial(multiple_simulations, server_ipaddr=server_ipaddr, server_port=server_port, webui=webui)

    pos = 0
    for i in range(total_procs):
        logger.debug(f"Worker {i} gets {batch_ranges[i]} number of simulation configurations")
        executor.submit(multiple_simulations_partial, batch[pos:pos+batch_ranges[i]])
        pos += batch_ranges[i]

    logger.debug(f"Waiting for the processes to finish")
    executor.shutdown(wait=True)
    logger.debug(f"The processes have finished without any errors")

    # Wait for the termination/confirmation from the progress server
    prog_sock.ref.recv(1)
