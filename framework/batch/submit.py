import argparse
from base64 import b64encode
from math import ceil, gcd
from multiprocessing import cpu_count
import os
from pathlib import Path
import socket
import subprocess
import sys

ELiSE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ELiSE_ROOT)

from batch.batch_utils import SchematicAnalysis
from common.utils import define_logger, is_bundled, process_name, get_executable

logger = define_logger()

def local_or_hpc_env() -> int:
    """
    Detects whether the code is running in a High-Performance Computing (HPC) environment or on a local machine.

    Returns:
        int: The total number of available cores for computation. This value will be used to set the number of processes
             when using multiprocessing.

    Raises:
        RuntimeError: If the total number of available cores is <= 0, this indicates an issue with the system configuration.
    """
    logger.debug("Checking if we are inside a scheduler environment")

    total_cores = -1

    if "SLURM_NTASKS" in os.environ:
        logger.debug("Inside a SLURM environment")
        total_cores = int(os.environ["SLURM_NTASKS"])
    else:
        logger.debug("Not in a scheduler environment. Executing in localhost")
        total_cores = cpu_count()

    if total_cores <= 0:
        logger.exception(f"The total amount of available cores is {total_cores} <= 0")
        raise RuntimeError(f"The total amount of available cores is {total_cores} <= 0")
    else:
        logger.debug(f"The total amount of available cores is {total_cores}")

    return total_cores

def calculate_for_less_avail_cores(total_procs: int, sim_configs: int) -> list[int]:
    """Calculate the number of simulation configurations that each process will handle.

    Parameters
    ----------
    total_procs     :   int
                        The total number of processes that will be launched for computation.

    sim_configs     :   int 
                        The total number of simulation configurations.

    Returns
    -------
    list[int]
        The list of simulation configurations for each process.
    """
    base = sim_configs // total_procs
    rem = sim_configs % total_procs
    return [base + 1 if i < rem else base for i in range(total_procs)]

def spawn_progress_server(server_ipaddr: str, server_port: int, batches: int, webui: bool) -> subprocess.Popen:
    """
    Starts a progress server process.

    Args:
        server_ipaddr (str): The IP address of the server.
        server_port (int): The port number of the server.
        clients (int): The maximum number of simulation configurations to connect to the server.

    Returns:
        subprocess.Popen: A Popen object representing the progress server process.

    Example:
        >>> server_ipaddr = "localhost"
        >>> server_port = 8080
        >>> clients = 100
        >>> sim_progress_proc = spawn_progress_server(server_ipaddr, server_port, clients)
    """
    logger.debug(f"Starting progress server process")
    
    # Get path for Progress Server
    root_path = Path(ELiSE_ROOT)
    if is_bundled():
        progress_server_path = root_path.parent / process_name("progress_server")
    else:
        progress_server_path = root_path / "batch" / process_name("progress_server")
    exe = get_executable(progress_server_path)

    # Create a command to run the "progress_server.py" script, passing in the required arguments
    server_prog_cmd = exe + ["--tcp_server_ipaddr", server_ipaddr, "--tcp_server_port", str(server_port), "--batches", str(batches)]

    # If running from WebUI then redirect all communication there
    if webui:
        server_prog_cmd.append("--webui")
    
    logger.debug(f"Progress server cmdline: {' '.join(server_prog_cmd)}")
    
    # Run the command as a subprocess
    sim_progress_proc = subprocess.Popen(server_prog_cmd, env=os.environ.copy())

    return sim_progress_proc

def spawn_simulation_runs(schematic_file: str, provider: str, server_ipaddr: str, server_port: int, sim_configs_num: int, webui: bool, export_reports: str) -> subprocess.Popen:
    """
    Spawn multiple simulation runs in parallel on localhost and optionally to remote machines using MPI.

    This function generates and executes a submission script to run the simulations
    in parallel using either Python's multiprocessing library (provider='mp') or MPI (provider='mpi').
    
    Args:
        schematic_file (str): Path to the schematic file containing simulation settings.
        provider (str): Backend provider, either 'mp' for multiprocessing or 'mpi' for Message Passing Interface.
        server_ipaddr (str): IP address of the progress server.
        server_port (int): Port number used for communication with the progress server.
        sim_configs_num (int): Number of simulation configurations.

    Returns:
        subprocess.Popen: The process object representing the submission command execution.
    """

    # Calculate the number of available cores under the context
    avail_cores = local_or_hpc_env()

    # Calculate the number of processes and batch range for each process based on available cores and number of simulation configurations
    if avail_cores >= sim_configs_num:
        total_procs = sim_configs_num
        batch_ranges = [1] * total_procs
    else:
        total_procs = avail_cores
        batch_ranges = calculate_for_less_avail_cores(total_procs, sim_configs_num)

    total_procs_str = f"One process" if total_procs == 1 else f"{total_procs} parallel processes"
    batch_str = f"a single simulation configuration" if sum(batch_ranges) == 1 else f"{sum(batch_ranges)} simulation configurations"
    logger.debug(f"{total_procs_str} for {batch_str}")

    # Encode the batch ranges
    branges_enc = b64encode(" ".join(map(lambda x: str(x), batch_ranges)).encode())
    branges_str = branges_enc.decode()

    logger.debug(f"Encoded the batch ranges: {branges_str} .")

    # Build the submission script depending on the provider
    submission_cmd = list()
    if provider == "mp":
        logger.debug("Using Python's multiprocessing library as backend")
        # Get path for running with multiprocessing library
        root_path = Path(ELiSE_ROOT)
        if is_bundled():
            run_mp_path = root_path.parent / process_name("run_mp")
        else:
            run_mp_path = root_path / "batch" / process_name("run_mp")
        exe = get_executable(run_mp_path)
        submission_cmd = exe + [schematic_file, str(total_procs), branges_str, server_ipaddr, str(server_port), export_reports]

    elif provider == "openmpi":
        logger.debug("Using OpenMPI as backend")
        # Get path for running with MPI library
        root_path = Path(ELiSE_ROOT)
        if is_bundled():
            run_mpi_path = root_path.parent / process_name("run_mpi")
        else:
            run_mpi_path = root_path / "batch" / process_name("run_mpi")
        exe = get_executable(run_mpi_path)
        submission_cmd = ["mpirun", "--bind-to", "none", "--oversubscribe", "-np", str(total_procs)] + exe + [schematic_file, branges_str, server_ipaddr, str(server_port), export_reports]

    elif provider == "intelmpi":
        logger.debug("Using IntelMPI as backend")

        # Get path for running with MPI library
        root_path = Path(ELiSE_ROOT)
        if is_bundled():
            run_mpi_path = root_path.parent / process_name("run_mpi")
        else:
            run_mpi_path = root_path / "batch" / process_name("run_mpi")
        exe = get_executable(run_mpi_path)
        # Intel MPI supports oversubscription by default
        # Not defining a bind policy places the threads randomly
        submission_cmd = ["mpiexec", "-np", str(total_procs)] + exe + [schematic_file, branges_str, server_ipaddr, str(server_port), export_reports]
    
    # Handle WebUI
    if webui:
        submission_cmd.append("1")
    else:
        submission_cmd.append("0")

    logger.debug(f"Submission cmdline: {' '.join(submission_cmd)}")

    logger.debug(f"Starting the simulation runs")

    sim_run_proc = subprocess.Popen(submission_cmd, env=os.environ.copy())
    
    return sim_run_proc

def execute_simulation(cmdargs=None):
    """
    Submit multiple simulation runs in the localhost or in an HPC environment given a schematic file and the vendor as input.

    Example usage:
        python submit.py -f my_schematic.yaml -p mpi
    """

    # Current supported providers. Might need to specify different MPI vendors (OpenMPI, IntelMPI)
    supported_providers = ["openmpi", "intelmpi", "mp"]

    parser = argparse.ArgumentParser(description="Provide a batch schematic file and a parallelizing provider to run simulations")
    parser.add_argument("-f", "--schematic-files", action="append", help="Provide (a) batch schematic file(s)", required=True)
    parser.add_argument("-p", "--provider", choices=supported_providers, default="mp", help="Define the provider for parallelizing tasks")
    parser.add_argument("--export_reports", default="", type=str, help="Provde a directory to export reports for each scheduler")
    parser.add_argument("--webui", default=False, action="store_true")

    if cmdargs is not None:
        args = parser.parse_args(cmdargs)
    else:
        args = parser.parse_args()

    schematic_files = args.schematic_files
    provider = args.provider
    export_reports = args.export_reports
    webui = args.webui


    # Get the IP address of the local machine that will launch both the progress server and the simulation runs
    # This will be broadcasted to the simulation run workers to report to the progress server
    server_ipaddr, server_port = socket.gethostbyname(socket.gethostname()), 54321

    # If we aren't using the WebUI, we spawn initially the progress server
    if not webui:
        sim_progress_proc = spawn_progress_server(server_ipaddr, server_port, len(schematic_files), webui)

    # And then spawn the batches
    sim_run_procs = list()
    for schematic_file in schematic_files:
        # Calculate the number of needed cores to run all the simulations in parallel
        schematic_analysis = SchematicAnalysis(schematic_file.strip())

        sim_configs = schematic_analysis.get_sim_configs_num()
        logger.debug(f"The total number of simulation configurations is {sim_configs}")

        sim_run_procs.append(
            spawn_simulation_runs(schematic_file.strip(), provider, server_ipaddr, server_port, sim_configs, webui, export_reports)
        )

    # We first wait for the batches of simulation configs
    for proc in sim_run_procs:
        proc.wait()
    logger.debug(f"The simulation runs finished successfully")

    # And then for the progress server if webui=False
    if not webui:
        sim_progress_proc.wait()
        logger.debug(f"The progress server closed gracefully")
    

if __name__ == "__main__":
    execute_simulation()
