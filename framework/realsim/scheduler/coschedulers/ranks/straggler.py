import os
import sys
import hashlib

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulers.ranks.randomranks import RandomRanksCoscheduler
from realsim.cluster.host import Host
from math import inf


class StragglerCoscheduler(RandomRanksCoscheduler):

    name = "Straggler Co-Scheduler"
    description = """Co-scheduler that tries to fill the ''holes'' 
    in the HPC system's resources created by the allocation of jobs inside"""

    def str_to_uniq_int(self, text):
        """Convert string to unique integer using MD5 hash"""
        hash_obj = hashlib.md5(text.encode())
        return int(hash_obj.hexdigest(), 16)

    def host_alloc_condition(self, hostname, job):
        # Get all the executing jobs in the host
        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If there are not then the execution will be spread and we want to
        # promote this
        if co_job_sigs == []:
            return (inf, inf) # but it can also set to return (inf, inf)..n things for sorting
        
        # Get the co-scheduled jobs as objects from the execution list
        co_jobs = [x for x in self.cluster.execution_list if x.get_signature() in co_job_sigs]

        # do not promote cases where more than 1 pairings happen within the same host/node
        if not len(co_jobs) == 1:
            return (-inf, inf)

        # the idea is to promote hosts that have a same processes job as the one we want to schedule,
        # so we try to fill the holes in the resources of the system (and also avoid straggler effect)
        # tie braker is the job name to make sure that the order is as less straggling as possible
        try:
            return (1/(co_jobs[0].num_of_processes-job.num_of_processes), self.str_to_uniq_int(co_jobs[0].job_name))
        except ZeroDivisionError:
            return (inf, self.str_to_uniq_int(co_jobs[0].job_name))
        
    # def waiting_queue_reorder(self, job: Job) -> float:
    #     # The job that is closer to cover the gaps is more preferrable
    #     sys_free_cores = self.cluster.get_idle_cores()
    #     if sys_free_cores > 0:
    #         diff = sys_free_cores - job.num_of_processes
    #         if diff > 0:
    #             factor0 = 1 - (diff/sys_free_cores)
    #         elif diff == 0:
    #             factor0 = 1
    #         else:
    #             factor0 = -1
    #     else:
    #         factor0 = 1

    #     factor1 = ((job.job_id + 1) / len(self.cluster.waiting_queue))

    #     return factor0 / factor1

    def waiting_queue_reorder(self, job: Job) -> float:

        for host in self.cluster.hosts.values():
            co_job_sigs = list(host.jobs.keys())
            if co_job_sigs == []:
                return 1

            co_jobs = [x for x in self.cluster.execution_list if x.get_signature() in co_job_sigs]
            for co_job in co_jobs:
                if co_job.num_of_processes == job.num_of_processes:
                    return 0.9
                
        return 0.0
    
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

            # Colocate
            if self.allocation(job, self.cluster.half_socket_allocation):
                deployed = True
                self.after_deployment()
 #               waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
 #                           reverse=True)
            #else:
            #    break

        return deployed