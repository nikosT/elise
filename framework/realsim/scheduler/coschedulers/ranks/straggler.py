import os
import sys

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

    def host_alloc_condition(self, hostname, job):
        # Get all the executing jobs in the host
        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If there are not then the execution will be spread and we want to
        # promote this
        if co_job_sigs == []:
            return inf # but it can also set to return (inf, inf)..n things for sorting
        
        cases = []
        for xjob in self.cluster.execution_list:
            sp_job_xjob = self.database.heatmap[job.job_name][xjob.job_name]
            sp_xjob_job = self.database.heatmap[xjob.job_name][job.job_name]

            if sp_job_xjob is None or sp_xjob_job is None:
                continue

            job_new_remaining_time = job.remaining_time / sp_job_xjob
            xjob_new_remaining_time = xjob.remaining_time / sp_xjob_job

            w_job = job.num_of_processes / sum(self.cluster.socket_conf)
            w_xjob = xjob.num_of_processes / sum(self.cluster.socket_conf)

            cases.append(abs(job_new_remaining_time*w_job - xjob_new_remaining_time*w_xjob))

        # because later sorting is in descending order (reverse=True)
        # and the metric need to be minimized
        return -max(cases)
            
