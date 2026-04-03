import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class RandomCompactCoscheduler(RanksCoscheduler):

    name = "Random Compact Co-Scheduler"
    description = """Random co-scheduling using compact allocation randomly based on the job's name
    to classic scheduling algorithms"""

    def host_alloc_condition(self, hostname: str, job: Job) -> float:
        return float(self.cluster.hosts[hostname].state != Host.IDLE)

    def deploy(self) -> bool:

        deployed = False

        # Update the rank of each job before scheduling them
        # self.update_ranks()

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            if 'compact' in job.job_name:
                # do compact if it is written in the job name, otherwise do colocate
                if self.compact_allocation(job, immediate=True):
                    deployed = True
                    self.after_deployment()
                else:
                    break

            else:
                # Colocate
                if self.allocation(job, self.cluster.half_socket_allocation):
                    deployed = True
                    self.after_deployment()
                else:
                    break

        return deployed
