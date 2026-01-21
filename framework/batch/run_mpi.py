from base64 import b64decode
from mpi4py import MPI
from functools import partial
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import import_module
from common.utils import define_logger
from common.communication import TCPSocket
from run_utils import multiple_simulations


mpi_comm = MPI.COMM_WORLD
mpi_comm.Set_errhandler(MPI.ERRORS_RETURN)
mpi_rank = mpi_comm.Get_rank()

# Define the server IP address and port number for all MPI ranks
server_ipaddr = sys.argv[3]
server_port = int(sys.argv[4])
export_reports = str(sys.argv[5])
webui = bool(int(sys.argv[6]))
multiple_simulations_partial = partial(multiple_simulations, server_ipaddr=server_ipaddr, server_port=server_port, webui=webui)

if mpi_rank == 0:

    # Define logger with log ancestry and environment details
    logger = define_logger(log_ancestry=True, log_env=True)

    schematic_file_path = sys.argv[1]

    branges = str(sys.argv[2])
    branges_dec = b64decode(branges.encode())
    branges_str = branges_dec.decode()
    batch_ranges = list(map(lambda x: int(x), branges_str.split()))

    total_procs = mpi_comm.Get_size()

    from batch.batch_utils import SchematicAnalysis
    schematic_analysis = SchematicAnalysis(schematic_file_path, webui)
    batch = schematic_analysis.batch

    logger.debug("Notify progress server for new batch of sim configs.")
    prog_sock = TCPSocket(server_ipaddr, server_port).client()
    prog_sock.send(msg={"type": "BatchStart", "batch_id": schematic_analysis.batch_id, "#configs": sum(batch_ranges), "export_reports": export_reports}, json_fmt=True)
    # TODO: Should check if the project name == batch id is unique

    if total_procs > 1:
        pos = batch_ranges[0]
        logger.debug("Start sending simulation configuration batches to other MPI ranks.")
        for i in range(1, total_procs):
 
            try:
                logger.debug(f"MPI Rank {i} gets {batch_ranges[i]} simulation configuration(s).")
                # Then, send the simulation batch
                mpi_comm.send(batch[pos:pos+batch_ranges[i]], dest=i)
                pos += batch_ranges[i]
            except:
                logger.exception(f"Problem occurred when sending simulation configurations batch from MPI Rank 0 to MPI Rank {i}.")

    logger.debug(f"Rank {mpi_rank} begins execution of simulation batches.")
    # Execute the simulation
    multiple_simulations_partial(batch[:batch_ranges[0]])

    # Wait for the termination/confirmation from the progress server
    prog_sock.ref.recv(1)

    logger.debug(f"Rank {mpi_rank} finished execution without any errors.")

else:

    logger = define_logger(log_ancestry=True, log_env=True)

    logger.debug(f"Rank {mpi_rank} begins execution of simulation batches.")
    # Execute the simulation
    multiple_simulations_partial(mpi_comm.recv(source=0))

    logger.debug(f"Rank {mpi_rank} finished execution without any errors.")
