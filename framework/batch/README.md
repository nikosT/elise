Configuration File
=====================

## Definition

- File in YAML format to configure the simulation
- Defines the workloads, schedulers and post-processing actions

## Format

```yaml
name: "Name of the project"
description: "[optional] Description of the project"

# Section for defining workloads (preferably unnamed)
workloads:
  - cluster:
      nodes: "Number (int) of nodes in a cluster"
      socket-conf: 
        - "The configuration of sockets in a node. Should be a list of ints"
    generator:
      type: "Type of generator (or a path to a Python file)"
      arg: "Argument (for Random the number of jobs, for Dict the name and frequency of loads and for List the path to the file containing the list)"
      distribution: "[optional] overrides the submit time of jobs based on a distribution"
        type: "Type of the distribution or path to .py file for submit times"
        arg: "Argument to pass to distribution"
    json: "[optional] Path to directory or file containing compact/coschedules or jobs (mutually exclusive with load-manager/db)"
    loads-machine: "Name of machine"
    loads-suite: "Name of suite"
    repeat: "Number (int) of how many times this workload will repeat"

# Section for defining schedulers and their options
schedulers:
  - backfill_enabled: "true/false — enable backfilling"
    base: "Name of scheduler or .py file"
    compact_fallback: "true/false — enable compact fallback scheduling"

# Section for defining actions after simulation
actions:
  get_workload:
    inputs: "all or list of workloads to include"
    schedulers: "all or list of scheduler names"
    dir: "Path to output directory for results"
  get_gantt_representation:
    inputs: "all or list of workloads to include"
    schedulers: "all or list of scheduler names"
    dir: "Path to output directory for results"
  get_waiting_queue:
    inputs: "all or list of workloads to include"
    schedulers: "all or list of scheduler names"
    dir: "Path to output directory for results"
  get_jobs_throughput:
    inputs: "all or list of workloads to include"
    schedulers: "all or list of scheduler names"
    dir: "Path to output directory for results"
  get_unused_cores:
    inputs: "all or list of workloads to include"
    schedulers: "all or list of scheduler names"
    dir: "Path to output directory for results"
```

## Example

```yaml
name: 'My Experiment'
description: 'Jupyter generated file'
inputs:
- cluster:
    nodes: 420
    socket-conf:
    - 10
    - 10
  generator:
    arg: 2
    type: 'Random Generator'
  json: "syn_NAS.json"
  loads-machine: "aris"
  loads-suite: 'NAS'
  repeat: 2
schedulers:
- backfill_enabled: false
  base: 'FIFO Scheduler'
  compact_fallback: false
- backfill_enabled: true
  base: 'EASY Scheduler'
  compact_fallback: false
- backfill_enabled: true
  base: 'Random Ranks Co-Scheduler'
  compact_fallback: false
actions:
  get_workload:
    inputs: 'all'
    schedulers: 'all'
    dir: "work_100_1758205436"
  get_gantt_representation:
    inputs: 'all'
    schedulers: 'all'
    dir: "work_100_1758205436"
  get_waiting_queue:
    inputs: "all"
    schedulers: "all"
    dir: "work_100_1758205436"
  get_jobs_throughput:
    inputs: "all"
    schedulers: "all"
    dir: "work_100_1758205436"
  get_unused_cores:
    inputs: "all"
    schedulers: "all"
    dir: "work_100_1758205436"
```

Submit Simulations with ELiSE
=====================================

This script is used to submit multiple simulation runs in parallel on localhost or in an HPC environment.
The scripts automatically handles the allocation of resources depending on the environment under which it is launched.

## Usage

To use this script, you need to provide a schematic file. You can also provide a parallelizing provider. The available providers are currently:
- "mpi" (Tested and validated only with OpenMPI)
- "mp" (Python's multiprocessing library), which is the default provider if no provider is specified.

The flag --export_reports can be used to export reports for each simulation run in a CSV file.

```bash
python elise.py -f <config_file> [-p <provider>] [--export_reports]
```

## Providers

Python's multiprocessing library is the default provider. It can only be used on localhost.

The MPI provider is tested and validated only with OpenMPI. It can be used on both localhost and HPC environments.
There are future considerations of using more MPI providers based on what is supported by mpi4py. The next item in the list is to test it with IntelMPI.

There is also work being done to support the debugging features that these MPI providers offer.

## Schedulers

Currently, the script is tested and validated only with the SLURM scheduler. There will be future considerations of supporting other schedulers, starting from non-commercial solutions.

### Examples

#### Example 1: Run simulations in parallel using MPI

```bash
python elise.py -f config.yaml -p mpi
```

This will launch multiple simulation runs in parallel using MPI on localhost or HPC environment.

#### Example 2: Run simulations in parallel using Python's multiprocessing library

```bash
python elise.py -f config.yaml -p mp
```

This will launch multiple simulation runs in parallel using Python's multiprocessing library.

#### Example 3: Export reports for each simulation run (requires export_reports=True)

```bash
python submit.py -f config.yaml -p mpi --export_reports
```

This will launch multiple simulation runs in parallel using MPI and export a report file for all simulation runs.
