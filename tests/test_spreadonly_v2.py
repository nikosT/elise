"""Varied-size synthetic fixtures exercise both branches of shared EASY logic."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import logging
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'framework'), str(ROOT)]
from batch.batch_utils import SchematicAnalysis, SimConfigCreator
from realsim.scheduler.coschedulers.spreadonly_v2 import SpreadOnlyV2Coscheduler
from realsim.scheduler.schedulers.easy import EASYScheduler


def run(config_path, output):
    output.mkdir(parents=True, exist_ok=True)
    for batch in SchematicAnalysis(str(config_path)).batch:
        _, _, _, sid, db, cluster, scheduler, logger, engine, _, _ = SimConfigCreator(batch).simconfig
        engine.debug_logger = logging.getLogger('test')
        cluster.setup(); scheduler.setup(); logger.setup()
        events = []
        original = scheduler.backfill
        def tracked():
            before = {j.job_id for j in cluster.execution_list}
            result = original()
            events.extend(dict(job_id=j.job_id, assigned_hosts=list(j.assigned_hosts))
                          for j in cluster.execution_list if j.job_id not in before)
            return result
        scheduler.backfill = tracked
        steps = 0
        while db.preloaded_queue or cluster.waiting_queue or cluster.execution_list:
            engine.sim_step()
            steps += 1
            if steps > 100: raise AssertionError('Simulation stalled')
        allocations = [dict(signature=sig, hosts=sorted(e['hosts']), start_s=e['start time'], finish_s=e['finish time'])
                       for sig,e in logger.job_events.items()]
        for host in cluster.hosts:
            intervals = sorted((a['start_s'], a['finish_s']) for a in allocations if host in a['hosts'])
            assert all(end <= nxt for (_,end),(nxt,_) in zip(intervals,intervals[1:])), 'Overlapping node allocations'
        assert cluster.idle_cores == cluster.total_cores
        (output/f'allocations_{sid}.json').write_text(json.dumps(allocations))
        (output/f'backfill_events_{sid}.json').write_text(json.dumps(events))
    return []


class EasyBackfillTests(unittest.TestCase):
    def simulate(self, sizes, times, spread, enabled=True, depth=100):
        # Node demand is identical across the compact/spread test fixtures.
        ppn = 2 if spread else 4
        name = 'Spread Only V2 Co-Scheduler' if spread else 'EASY Scheduler'
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root=Path(tmp)
            loads=[]
            for i,(nodes,runtime) in enumerate(zip(sizes,times)):
                loads.append(dict(load_name=f'app{i}',num_of_processes=nodes*ppn,
                    machine='synthetic',suite='test',compact_timelogs=[runtime],spread_timelogs=[runtime],
                    coscheduled_timelogs={f'app{j}':[[runtime]] for j in range(len(sizes))},
                    dpops=0,bytes_transferred=0,ipc=0,compute_time_norm=0,mpi_time_norm=0))
            pool=root/'pool.json'
            pool.write_text(json.dumps(dict(machine='synthetic',suite='test',loads=list(map(json.dumps,loads)))))
            text=','.join(['field']*18)+'\n'
            for i,(nodes,runtime) in enumerate(zip(sizes,times)):
                # A small walltime margin avoids the engine's abort-at-equality rule.
                text+=','.join(map(str,[i,0,0,0,nodes*ppn,'','',nodes*ppn,runtime+.1,'',1,'','',f'app{i}','','','','']))+'\n'
            config={'name':'test','inputs':[{'json':str(pool),'cluster':{'nodes':3,'socket-conf':[2,2]},
                    'generator':{'type':'List Generator','arg':text}}],
                    'schedulers':[{'base':name,'backfill_enabled':enabled,'backfill_depth':depth}],'actions':{}}
            path=root/'config.yaml';path.write_text(json.dumps(config))
            metrics=run(path,root/'out')
            allocations=json.loads((root/'out/allocations_0.json').read_text())
            events=json.loads((root/'out/backfill_events_0.json').read_text())
            return {int(a['signature'].split(':')[0]):a for a in allocations},events,metrics

    def test_backfill_enabled_by_default(self):
        self.assertTrue(SpreadOnlyV2Coscheduler().backfill_enabled)
        self.assertEqual(SpreadOnlyV2Coscheduler().backfill_depth, 100)

    def test_backfill_mirrors_easy_with_spread_substitutions(self):
        import ast, inspect, textwrap
        expected = textwrap.dedent(inspect.getsource(EASYScheduler.backfill))
        expected = expected.replace('full_socket_nodes', 'half_socket_nodes').replace('compact_allocation', 'spread_allocation')
        actual = textwrap.dedent(inspect.getsource(SpreadOnlyV2Coscheduler.backfill))
        actual = actual.replace('    if not self.backfill_enabled:\n        return False\n\n', '')
        self.assertEqual(ast.dump(ast.parse(actual)), ast.dump(ast.parse(expected)))

    def test_short_job_uses_reserved_idle_node_without_delaying_head(self):
        for spread in [False,True]:
            with self.subTest(spread=spread):
                a,e,_=self.simulate([1,1,2,1],[100,10,10,5],spread)
                self.assertEqual(a[3]['start_s'],0)
                self.assertEqual(a[2]['start_s'],10)
                self.assertEqual(len(e),1)
                self.assertEqual(e[0]['assigned_hosts'], ['host2'])

    def test_long_job_cannot_use_reserved_idle_node(self):
        for spread in [False,True]:
            a,e,_=self.simulate([1,1,2,1],[100,10,10,20],spread)
            self.assertEqual(a[2]['start_s'],10)
            self.assertEqual(a[3]['start_s'],20)
            self.assertEqual(e,[])

    def test_long_job_can_use_nonreserved_idle_node(self):
        for spread in [False,True]:
            a,e,_=self.simulate([2,2,1],[10,10,20],spread)
            self.assertEqual(a[2]['start_s'],0)
            self.assertEqual(a[1]['start_s'],10)
            self.assertEqual(e[0]['assigned_hosts'], ['host2'])

    def test_disabled_backfill_and_zero_depth_preserve_fifo(self):
        for spread in [False,True]:
            for enabled,depth in [(False,100),(True,0)]:
                a,e,_=self.simulate([1,1,2,1],[100,10,10,5],spread,enabled,depth)
                self.assertEqual(a[3]['start_s'],20)
                self.assertEqual(e,[])


if __name__=='__main__': unittest.main()
