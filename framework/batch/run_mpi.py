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

class MPITransferTag:
    MODULES = 10
    SIMBATCH = 20

mpi_comm = MPI.COMM_WORLD
mpi_rank = mpi_comm.Get_rank()

# Define the server IP address and port number for all MPI ranks
server_ipaddr = sys.argv[3]
server_port = int(sys.argv[4])
webui = bool(int(sys.argv[5]))
multiple_simulations_partial = partial(multiple_simulations, server_ipaddr=server_ipaddr, server_port=server_port, webui=webui)

if mpi_rank == 0:

    # Define logger with log ancestry and environment details
    logger = define_logger(log_ancestry=True, log_env=True)

    schematic_file_path = sys.argv[1]
    batch_size = int(sys.argv[2])
    total_procs = mpi_comm.Get_size()

    from batch.batch_utils import BatchCreator
    batch_creator = BatchCreator(schematic_file_path, webui)
    batch_creator.create_ranks()

    logger.debug("Notify progress server for new batch of sim configs.")
    prog_sock = TCPSocket(server_ipaddr, server_port).reusable().client()
    prog_sock.send(msg={"type": "BatchStart", "batch_id": batch_creator.batch_id, "#configs": str(total_procs)}, json_fmt=True)
    # TODO: Should check if the project name == batch id is unique

    if total_procs > 1:
        logger.debug("Start sending simulation configuration batches to other MPI ranks")
        for i in range(1, total_procs):
            try:
                logger.debug("Send the additional module that need to load")
                # Send the necessary modules to import first
                mpi_comm.send(batch_creator.mods_export, dest=i, tag=MPITransferTag.MODULES)
            except:
                logger.exception(f"Problem occurred when sending modules to be imported from MPI Rank 0 to MPI Rank {i}")
 
            try:
                logger.debug(f"MPI Rank {i} gets {batch_size} number of simulation configurations")
                # Then, send the simulation batch
                mpi_comm.send(batch_creator.ranks[i*batch_size:(i+1)*batch_size], dest=i, tag=MPITransferTag.SIMBATCH)
            except:
                logger.exception(f"Problem occurred when sending simulation configurations batch from MPI Rank 0 to MPI Rank {i}")

    logger.debug(f"Rank {mpi_rank} begins execution of simulation batches")
    # Execute the simulation
    multiple_simulations_partial(batch_creator.ranks[:batch_size])

    logger.debug(f"Rank {mpi_rank} finished execution without any errors")

    # Wait for the termination/confirmation from the progress server
    prog_sock.ref.recv(1)

else:

    logger = define_logger(log_ancestry=True, log_env=True)

    # Import all the necessary modules before starting the simulation
    necessary_modules = mpi_comm.recv(source=0, tag=MPITransferTag.MODULES)
    logger.debug(f"Rank {mpi_rank} receives modules to import: {necessary_modules}")
    for mod in necessary_modules:
        import_module(mod)

    logger.debug(f"Rank {mpi_rank} begins execution of simulation batches")
    # Execute the simulation
    multiple_simulations_partial(mpi_comm.recv(source=0, tag=MPITransferTag.SIMBATCH))

    logger.debug(f"Rank {mpi_rank} finished execution without any errors")
