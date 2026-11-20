"""Regression tests for the two phases of MPI straggler co-location.

Run: python -m unittest discover -s tests -p 'test_straggler_interference.py' -v
"""
import logging
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "framework"))

try:
    import procset
except ImportError:
    stub = types.ModuleType("procset")
    stub.ProcSet = type("ProcSet", (), {})
    sys.modules["procset"] = stub

from realsim.compengine import ComputeEngine
from realsim.jobs.jobs import Job, JobState


class DummyLogger:
    def log(self, *args, **kwargs):
        pass


class StragglerTest(unittest.TestCase):
    def setUp(self):
        self.hosts = {h: SimpleNamespace(jobs={}) for h in ("h0", "h1")}
        self.cluster = SimpleNamespace(
            hosts=self.hosts, socket_conf=(4,), execution_list=[],
            waiting_queue=[], makespan=0.0
        )
        self.db = SimpleNamespace(preloaded_queue=[], heatmap={})
        self.engine = ComputeEngine(
            self.db, self.cluster, SimpleNamespace(), DummyLogger()
        )
        self.engine.debug_logger = logging.getLogger(__name__)

        # Isolate the timing engine from ProcSet resource cleanup.
        def cleanup(job):
            job.finish_time = self.cluster.makespan
            if job.current_state != JobState.ABORTED:
                job.current_state = JobState.FINISHED
            for host in job.assigned_hosts:
                self.hosts[host].jobs.pop(job.get_signature())
        self.engine.clean_job_from_hosts = cleanup

    def add(self, ident, name, hosts, work=100, wall=5000):
        job = Job(ident, name, 4, list(hosts), work, 0, 0, wall)
        job.start_time = self.cluster.makespan
        job.socket_conf = (2,)
        job.current_state = JobState.EXECUTING
        job.host_remaining_work = {host: float(work) for host in hosts}
        self.cluster.execution_list.append(job)
        for host in hosts:
            self.hosts[host].jobs[job.get_signature()] = []
        return job

    def test_inactive_portion_relaxes_interference_without_releasing_cores(self):
        a = self.add(0, "A", ("h0", "h1"))
        b = self.add(1, "B", ("h0",))
        b.spread_speedup = 2.0
        self.add(2, "C", ("h1",), work=1000)
        self.db.heatmap = {
            "A": {"B": 1.5, "C": 0.5},
            "B": {"A": 0.8},
            "C": {"A": 1.0},
        }

        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(self.cluster.makespan, 100 / 1.5)
        self.assertEqual(a.host_remaining_work["h0"], 0)
        self.assertGreater(a.host_remaining_work["h1"], 0)
        self.assertIn(a.get_signature(), self.hosts["h0"].jobs)
        self.assertIn(a, self.cluster.execution_list)

        peers = {j.get_signature(): j for j in self.cluster.execution_list}
        self.assertAlmostEqual(self.engine._host_speedup(b, "h0", peers), 2.0)
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(
            b.finish_time,
            100 / 1.5 + (100 - 0.8 * (100 / 1.5)) / 2
        )
        self.assertIn(a, self.cluster.execution_list)

        while a in self.cluster.execution_list:
            self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 200)

    def test_equal_host_progress(self):
        a = self.add(0, "A", ("h0", "h1"))
        a.spread_speedup = 2.0
        self.db.heatmap = {"A": {}}
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 50)
        self.assertEqual(a.host_completion_times, {"h0": 50, "h1": 50})

    def test_unknown_pair_uses_average(self):
        a = self.add(0, "A", ("h0",))
        self.add(1, "B", ("h0",), work=1000)
        a.avg_speedup = 0.75
        self.db.heatmap = {"A": {"B": None}, "B": {"A": 1.0}}
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 100 / 0.75)

    def test_arrival_does_not_reset_progress(self):
        a = self.add(0, "A", ("h0", "h1"))
        a.spread_speedup = 2.0
        self.db.heatmap = {"A": {"B": 0.5}, "B": {"A": 1.0}}
        self.db.preloaded_queue = [SimpleNamespace(submit_time=30)]
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.host_remaining_work["h0"], 40)
        self.db.preloaded_queue.clear()
        self.add(1, "B", ("h1",), work=1000)
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(self.cluster.makespan, 50)
        self.assertEqual(a.host_remaining_work["h0"], 0)
        self.assertAlmostEqual(a.host_remaining_work["h1"], 30)

    def test_missing_spread_uses_best_pair(self):
        a = self.add(0, "A", ("h0",))
        self.db.heatmap = {
            "A": {"B": 1.4, "C": 1.8, "D": None, "E": float("nan")}
        }
        peers = {a.get_signature(): a}
        self.assertIsNone(a.spread_speedup)
        self.assertAlmostEqual(self.engine._host_speedup(a, "h0", peers), 1.8)
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 100 / 1.8)

    def test_explicit_spread_preferred(self):
        a = self.add(0, "A", ("h0",))
        a.spread_speedup = 2.25
        self.db.heatmap = {"A": {"B": 1.2, "C": 1.8}}
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 100 / 2.25)

    def test_no_spread_or_pairs_uses_one(self):
        a = self.add(0, "A", ("h0",))
        self.db.heatmap = {"A": {"B": None}}
        self.engine.goto_next_sim_state()
        self.assertAlmostEqual(a.finish_time, 100)

    def test_wall_time_expiry(self):
        a = self.add(0, "A", ("h0",), wall=20)
        self.db.heatmap = {"A": {}}
        self.engine.goto_next_sim_state()
        self.assertEqual(a.current_state, JobState.ABORTED)
        self.assertAlmostEqual(a.finish_time, 20)
        self.assertNotIn(a.get_signature(), self.hosts["h0"].jobs)

    def test_deepcopy_independent_work(self):
        a = self.add(0, "A", ("h0",))
        a.host_remaining_work["h0"] = 25
        a.spread_speedup = 1.7
        copy = a.deepcopy()
        copy.host_remaining_work["h0"] = 10
        self.assertEqual(a.host_remaining_work["h0"], 25)
        self.assertEqual(copy.spread_speedup, 1.7)


if __name__ == "__main__":
    unittest.main()
