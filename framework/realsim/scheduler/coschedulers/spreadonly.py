"""Exclusive-node spreading using Elise's two-way resource model."""
from math import ceil, isfinite

from procset import ProcSet

from realsim.cluster.host import Host
from realsim.scheduler.coscheduler import Coscheduler
from realsim.scheduler.scheduler import Scheduler


class SpreadOnlyCoscheduler(Coscheduler):
    name = "Spread Only Co-Scheduler"
    description = "FIFO two-way spreading on idle nodes, without a co-located partner"

    def setup(self):
        Scheduler.setup(self)
        if any(n <= 0 or n % 2 for n in self.cluster.full_socket_allocation):
            raise ValueError("Spread Only requires positive, even socket core counts")
        if self.backfill_enabled:
            raise ValueError("Spread Only implements strict FIFO; disable backfill")
        for job in self.database.preloaded_queue:
            speedup = job.spread_speedup
            if speedup is None or not isfinite(speedup) or speedup <= 0:
                raise ValueError(f"Missing positive measured spread speedup: {job.job_name}")
            if ceil(job.num_of_processes / sum(self.cluster.half_socket_allocation)) > self.cluster.nodes:
                raise ValueError(f"Spread job cannot fit the cluster: {job.job_name}")
            # ComputeEngine uses max_speedup for a spread job without neighbors.
            # Its setup has already widened that maximum using the pair heatmap.
            # Restore the measured value for this policy only (including < 1).
            job.max_speedup = speedup

    def find_suitable_nodes(self, req_cores, socket_conf, immediate=False):
        """Leave the other half idle and unavailable to subsequent jobs."""
        hosts = {}
        for hostname, host in self.cluster.hosts.items():
            if host.state != Host.IDLE or host.jobs:
                continue
            hosts[hostname] = [
                ProcSet.from_str(' '.join(map(str, pset[:count])))
                for pset, count in zip(host.sockets, socket_conf)
            ]
            req_cores -= sum(socket_conf)
            if immediate and req_cores <= 0:
                break
        return hosts, req_cores <= 0

    def deploy(self):
        deployed = False
        for job in list(self.cluster.waiting_queue[:self.queue_depth]):
            if not self.allocation(job, self.cluster.half_socket_allocation, immediate=True):
                break
            deployed = True
        return deployed
