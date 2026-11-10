"""Version 2: exclusive spread allocation with the branch's EASY backfill rule.

The original easy.py and spreadonly.py are left unchanged. The backfill method
mirrors EASYScheduler.backfill, substituting spread node counts/allocation only.
"""
from math import inf

from procset import ProcSet

from realsim.cluster.host import Host
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.spreadonly import SpreadOnlyCoscheduler


class SpreadOnlyV2Coscheduler(SpreadOnlyCoscheduler):
    name = "Spread Only V2 Co-Scheduler"
    description = "Exclusive two-way spread allocation with optional EASY backfill"

    def __init__(self):
        super().__init__()
        self.backfill_enabled = True
        self.backfill_depth = 100

    def setup(self):
        # Reuse V1's validation and measured-speedup setup without its FIFO-only
        # guard. Restore the configured flag even if validation raises an error.
        enabled = self.backfill_enabled
        try:
            self.backfill_enabled = False
            super().setup()
        finally:
            self.backfill_enabled = enabled

    def spread_allocation(self, job, immediate=False, omit_hosts=None):
        return self.allocation(job, self.cluster.half_socket_allocation,
                               immediate=immediate, omit_hosts=omit_hosts or [])

    def find_suitable_nodes(self, req_cores, socket_conf, immediate=False, omit_hosts=None):
        """Leave the other half idle and unavailable to subsequent jobs."""
        hosts = {}
        omitted = set(omit_hosts or [])
        for hostname, host in self.cluster.hosts.items():
            if hostname in omitted or host.state != Host.IDLE or host.jobs:
                continue
            hosts[hostname] = [
                ProcSet.from_str(' '.join(map(str, pset[:count])))
                for pset, count in zip(host.sockets, socket_conf)
            ]
            req_cores -= sum(socket_conf)
            if immediate and req_cores <= 0:
                break
        return hosts, req_cores <= 0

    def backfill(self) -> bool:

        if not self.backfill_enabled:
            return False

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            return False

        execution_list = deepcopy_list(self.cluster.execution_list)
        execution_list.sort(key=lambda job: job.wall_time + job.start_time - self.cluster.makespan)

        blocked_job = self.cluster.waiting_queue[0]

        # Get all the idle hosts
        idle_hosts = [host for host in list(self.cluster.hosts.values()) if host.state == Host.IDLE]

        idle_hostnames = [
            hostname
            for hostname, host in self.cluster.hosts.items()
            if host.state == Host.IDLE
        ]
        # Find the minimum estimated start time of the job

        # Calculate the number of idle hosts needed
        aggr_hosts = len(idle_hosts)
        min_estimated_time = inf

        eligible_hosts = idle_hostnames

        for xjob in execution_list:

            aggr_hosts += len(xjob.assigned_hosts)

            eligible_hosts.extend(xjob.assigned_hosts)

            if aggr_hosts >= blocked_job.half_socket_nodes:
                blocked_hosts = sorted(
                    eligible_hosts,
                    key=lambda x: int(x.replace('host', ''))
                )[:blocked_job.half_socket_nodes]

                min_estimated_time = xjob.wall_time - (self.cluster.makespan - xjob.start_time)
                break

        # If a job couldn't reserve cores then cancel backfill at this point
        if not min_estimated_time < inf:
            return False

        # Find job(s) that can backfill the execution list

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:self.backfill_depth+1])

        # Scan through the rest of the jobs to see if any is fit for backfilling
        for b_job in backfilling_jobs:
            time_clause = False
            if b_job.wall_time <= min_estimated_time:
                time_clause = True

            non_reserved_idle_hosts = [
                host for host in idle_hostnames
                if host not in blocked_hosts
            ]

            can_fit_without_reserved = (
                len(non_reserved_idle_hosts)
                >= b_job.half_socket_nodes
            )

            if not can_fit_without_reserved:

                if time_clause:
                    if self.spread_allocation(b_job, immediate=True):
                        deployed = True
                else:
                    deployed = False
            else:
                if time_clause:
                    if self.spread_allocation(b_job, immediate=True):
                        deployed = True
                else:
                    if self.spread_allocation(b_job, immediate=True, omit_hosts=list(set(blocked_hosts) & set(idle_hostnames))):
                        deployed = True

        return deployed

