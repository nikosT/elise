# Utilities
from math import inf, ceil, isfinite
import numpy as np
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../")
))
from common.utils import define_logger

# Simulation
from procset import ProcSet
from realsim.jobs.jobs import Job, JobCharacterization, JobState
from realsim.jobs.utils import deepcopy_list
from realsim.database import Database
from realsim.cluster.host import Host
from realsim.cluster.cluster import Cluster
from realsim.logger.logger import Logger
import realsim.logger.logevts as evts



class ComputeEngine:
    
    def __init__(self, 
                 db: Database,
                 cluster: Cluster,
                 scheduler,
                 logger: Logger):

        self.db = db
        self.cluster = cluster
        self.scheduler = scheduler
        self.logger = logger

        self.scheduler.database = self.db
        self.scheduler.cluster = self.cluster
        self.scheduler.logger = self.logger
        self.scheduler.compeng = self

        self.logger.database = self.db
        self.logger.cluster = self.cluster
        self.logger.scheduler = self.scheduler

        self.debug_logger = None
        
        # Features for compengine
        self.wall_time_ratio = 0

    # Database preloaded queue setup
    def setup_preloaded_jobs(self) -> None:
        """Setup the preloaded jobs that are currently stored in the database
        """

        # Sort jobs by their time they will be appearing in the waiting queue
        self.db.preloaded_queue.sort(key=lambda job: job.submit_time)
        
        # Get the submit time of the first job and subtract it from the other jobs
        # We are shifting them to start = 0
        first_submit_time = self.db.preloaded_queue[0].submit_time

        # Preload jobs and calculate their respective half and full node cores
        # usage
        for job in self.db.preloaded_queue:

            # Shift the job by first_submit_time amount
            job.submit_time -= first_submit_time
            
            # Set job id
            job.job_id = self.cluster.id_counter

            # Setup core resources needed
            job.full_socket_nodes = ceil(job.num_of_processes / sum(self.cluster.full_socket_allocation))
            job.half_socket_nodes = ceil(job.num_of_processes / sum(self.cluster.half_socket_allocation))

            # Setup job speedups
            try:
                speedups = list(self.db.heatmap[job.job_name].values())
                speedups = [spd for spd in speedups if spd is not None]
            except:
                # Set everything to compact speedup if no list is given
                speedups = [1]
            
            # Given by the LoadManager during the creation of the job
            # The heatmap during a simulation run might be different from the generated heatmap by the LoadManager
            min_speedup = job.min_speedup
            max_speedup = job.max_speedup

            if not max_speedup:
                min_speedup = max_speedup = speedups[0]

            accumulator = length = 0
            for speedup in speedups:
                if speedup > max_speedup:
                    max_speedup = speedup
                if speedup < min_speedup:
                    min_speedup = speedup

                accumulator += speedup
                length += 1

            job.max_speedup = max_speedup
            job.min_speedup = min_speedup
            job.avg_speedup = (accumulator / length)

            # Setup job characterization
            avg = job.avg_speedup
            std = round(float(np.std(speedups)), 2)

            if avg > 1.02:
                job.job_character = JobCharacterization.SPREAD
            elif avg < 0.98:
                job.job_character = JobCharacterization.COMPACT
            else:
                if std > 0.07:
                    job.job_character = JobCharacterization.FRAIL
                else:
                    job.job_character = JobCharacterization.ROBUST

            self.cluster.id_counter += 1

    def load_in_waiting_queue(self) -> None:

        copy = deepcopy_list(self.db.preloaded_queue)

        for job in copy:
            if job.submit_time <= self.cluster.makespan:
                # Infinite waiting queue size
                # if self.cluster.queue_size == -1:
                #     self.db.preloaded_queue.remove(job)
                #     self.cluster.waiting_queue.append(job)

                # # Zero size waiting queue
                # elif self.cluster.queue_size == 0 and\
                #         len(self.cluster.waiting_queue) == 0 and\
                #         self.scheduler.find_suitable_nodes(job.num_of_processes, self.cluster.full_socket_allocation) != {}:
                #             self.db.preloaded_queue.remove(job)
                #             job.submit_time = self.cluster.makespan
                #             self.cluster.waiting_queue.append(job)

                # # Finite waiting queue size but not 0
                # elif self.cluster.queue_size > 0 and len(self.cluster.waiting_queue) < self.cluster.queue_size:
                self.db.preloaded_queue.remove(job)
                job.submit_time = self.cluster.makespan
                self.cluster.waiting_queue.append(job)

                #else:
                #    break

    # Job execution/deploying/cleaning computations
    def _host_speedup(self, job: Job, hostname: str, executing_jobs: dict) -> float:
        """Speedup of the active portion of a job on a given host.

        Finished local portions remain allocated, but no longer impose
        computational interference on jobs co-located on that host.
        """
        if job.socket_conf == self.cluster.socket_conf:
            return 1.0  # Compact exclusive allocation

        if job.spread_speedup is not None:
            if not isfinite(job.spread_speedup) or job.spread_speedup <= 0:
                raise ValueError(f"Invalid spread speedup for {job.get_signature()}")
            speedup = job.spread_speedup
        else:
            # No measured spread speedup: approximate using the best
            # available co-location speedup for this application.
            valid = [
                value for value in self.db.heatmap.get(job.job_name, {}).values()
                if isinstance(value, (int, float))
                and isfinite(value) and value > 0
            ]
            speedup = max(valid, default=1.0)

        for signature in self.cluster.hosts[hostname].jobs:
            if signature == job.get_signature():
                continue
            neighbor = executing_jobs.get(signature)
            if neighbor is None or neighbor.host_remaining_work.get(hostname, 0) <= 0:
                continue
            pair_speedup = self.db.heatmap[job.job_name][neighbor.job_name]
            if pair_speedup is None:
                pair_speedup = job.avg_speedup
            speedup = min(speedup, pair_speedup)

        if speedup <= 0:
            raise ValueError(f"Non-positive speedup for {job.get_signature()} on {hostname}")
        return speedup

    def calculate_job_rem_time(self, job: Job, executing_jobs: dict = None) -> None:
        """Estimate completion from the last active host portion.

        Host work is tracked in compact-equivalent execution seconds.
        """
        if executing_jobs is None:
            executing_jobs = {
                other.get_signature(): other for other in self.cluster.execution_list
            }
        if not job.host_remaining_work and job.assigned_hosts:
            job.host_remaining_work = {
                host: float(job.remaining_time) for host in job.assigned_hosts
            }

        job.host_speedups = {}
        remaining = 0.0
        critical_speedup = job.sim_speedup
        for hostname, work in job.host_remaining_work.items():
            if work <= 0:
                continue
            speedup = self._host_speedup(job, hostname, executing_jobs)
            job.host_speedups[hostname] = speedup
            local_remaining = work / speedup
            if local_remaining > remaining:
                remaining = local_remaining
                critical_speedup = speedup

        job.remaining_time = remaining
        job.sim_speedup = critical_speedup

    def deploy_job_to_host(self, hostname: str, job: Job, psets: list[ProcSet]) -> None:


        # Store hostname in job's registry
        job.assigned_hosts.append(hostname)

        # Add job signature to the host and the processor set it allocates
        self.cluster.hosts[hostname].jobs.update({
            job.get_signature(): psets
        })

        # Remove psets from host and decrease the number of idle cores in cluster
        for i, socket_pset in enumerate(self.cluster.hosts[hostname].sockets):
            socket_pset -= psets[i]
            self.cluster.idle_cores -= len(psets[i])

        # Log the event
        self.logger.log(evts.JobStart, msg=job.get_signature(), job=job, psets=psets, hostname=hostname)
        self.logger.log(evts.JobDeployedToHost, msg=f"{job.get_signature()} in-> {hostname}")

        self.debug_logger.debug(f"Job {job.get_signature()} is deployed to host {hostname}")

    def deploy_job_to_hosts(self, suitable_hosts, job) -> None:

        self.debug_logger.debug(f"Job {job.get_signature()} is being deployed")

        # Remove job from cluster's waiting queue
        self.cluster.waiting_queue.remove(job)

        job.current_state = JobState.EXECUTING
        job.start_time = self.cluster.makespan

        for hostname, psets in suitable_hosts:
            # Deploy job
            self.deploy_job_to_host(hostname, job, psets)
            # Set host state as allocated
            self.cluster.hosts[hostname].state = Host.ALLOCATED

        # Each host initially owns the same compact-equivalent work.
        job.host_remaining_work = {
            hostname: float(job.remaining_time) for hostname in job.assigned_hosts
        }
        job.host_speedups = {}
        job.host_completion_times = {}

        # Add job to the executing list
        self.cluster.execution_list.append(job)

        self.debug_logger.debug(f"Job {job.get_signature()} has deployed for execution")

    def clean_job_from_hosts(self, job: Job) -> None:

        self.debug_logger.debug(f"Job {job.get_signature()} is being cleaned from allocated hosts")

        # Set the finish time of the job
        job.finish_time = self.cluster.makespan
        
        if job.current_state not in (JobState.ABORTED, JobState.FAILED):
            job.current_state = JobState.FINISHED

        # Clean job and return resources back to host
        for hostname in job.assigned_hosts:
            # Log the event
            self.logger.log(evts.JobCleanedFromHost, msg=f"{hostname} out-> {job.get_signature()}")

            # Return the allocated processors of a job to each host 
            # and add the number of returned cores to idle cores of cluster
            for i, pset in enumerate(self.cluster.hosts[hostname].jobs[job.get_signature()]):
                self.cluster.hosts[hostname].sockets[i] = self.cluster.hosts[hostname].sockets[i].union(pset)
                self.cluster.idle_cores += len(pset)

            # Remove job signature from host
            self.cluster.hosts[hostname].jobs.pop(job.get_signature())
            
            # Change state of host if nothing is executing
            if len(list(self.cluster.hosts[hostname].jobs.keys())) == 0:
                self.cluster.hosts[hostname].state = Host.IDLE
 
        # Log the event
        self.logger.log(evts.JobFinish, msg=f"{job.get_signature()}", job=job)

        self.debug_logger.debug(f"Job {job.get_signature()} has been cleaned from allocated hosts")


    # Simulation loop computations
    def goto_next_sim_state(self) -> None:
        """Advance to the next arrival, local completion, or wall-time event."""
        executing_jobs = {
            job.get_signature(): job for job in self.cluster.execution_list
        }
        min_step = inf

        # Compute speedups before changing any work, so all jobs advance
        # at rates derived from the same instantaneous interference state.
        for job in self.cluster.execution_list:
            self.calculate_job_rem_time(job, executing_jobs)
            for hostname, work in job.host_remaining_work.items():
                if work > 0:
                    min_step = min(min_step, work / job.host_speedups[hostname])

            until_deadline = (
                job.start_time + job.wall_time * (1 + self.wall_time_ratio)
                - self.cluster.makespan
            )
            if until_deadline > 0:
                min_step = min(min_step, until_deadline)

        for job in self.db.preloaded_queue:
            until_arrival = job.submit_time - self.cluster.makespan
            if until_arrival > 0:
                min_step = min(min_step, until_arrival)
                break

        if min_step == inf:
            if self.cluster.waiting_queue or self.db.preloaded_queue:
                raise RuntimeError("Jobs queued but none can advance")
            return

        assert min_step > 0
        self.cluster.makespan += min_step
        self.logger.log(evts.CompEngineNextTimeStep, msg=f"{min_step}")

        still_executing = []
        for job in self.cluster.execution_list:
            for hostname, work in job.host_remaining_work.items():
                if work <= 0:
                    continue
                remaining = work - min_step * job.host_speedups[hostname]
                if remaining <= 1e-9:
                    remaining = 0.0
                    job.host_completion_times[hostname] = self.cluster.makespan
                    self.logger.log(
                        evts.JobHostComputeComplete,
                        msg=f"{job.get_signature()} on {hostname}",
                    )
                job.host_remaining_work[hostname] = remaining

            job.remaining_time = max(0.0, job.remaining_time - min_step)
            if (job.start_time + job.wall_time * (1 + self.wall_time_ratio)
                    <= self.cluster.makespan + 1e-9):
                job.current_state = JobState.ABORTED
                job.remaining_time = 0.0

            if job.current_state == JobState.ABORTED or all(
                    work <= 0 for work in job.host_remaining_work.values()):
                job.remaining_time = 0.0
                self.clean_job_from_hosts(job)
            else:
                still_executing.append(job)

        self.cluster.execution_list = still_executing

    def sim_step(self) -> None:

        self.debug_logger.debug("Begin of a simulation step")

        deployed = False

        # Deploy to waiting queue any preloaded jobs that remain
        self.debug_logger.debug("Loading any job(s) that arrived in the waiting queue")
        self.load_in_waiting_queue()
        
        # Check if there are any jobs left waiting
        if self.cluster.waiting_queue != []:

            # Deploy/Submit jobs to the execution list
            deployed = self.scheduler.deploy()

            # If the backfilling policy of a scheduler is enabled
            if self.scheduler.backfill_enabled:

                # Execute the backfilling algorithm
                deployed |= self.scheduler.backfill()

        # If deployed restart scheduling procedure
        # if deployed:
        #     self.debug_logger.debug("End of a simulation step")
        #     return

        # If the scheduler didn't deploy jobs then
        # 1. the cluster's execution list is full
        # 2. There are no jobs in the waiting queue but there are in the 
        #    queue preloaded
        self.goto_next_sim_state()
        
        self.debug_logger.debug("End of a simulation step")
