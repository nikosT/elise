# Spread Only V2 with optional EASY backfill

Select `Spread Only V2 Co-Scheduler` in an Elise project configuration.
It reserves exclusive nodes and uses half of each socket; no other job may
occupy the unused half. It uses measured `spread_timelogs`, including speedups
below one, independently of synthetic pairwise co-location values.

## Configuration

With EASY backfill (enabled by default in V2):

```yaml
schedulers:
  - base: EASY Scheduler
    backfill_enabled: true
    backfill_depth: 100
  - base: Spread Only V2 Co-Scheduler
    backfill_enabled: true
    backfill_depth: 100
```

Without backfill, for a FIFO comparison:

```yaml
schedulers:
  - base: FIFO Scheduler
    backfill_enabled: false
  - base: Spread Only V2 Co-Scheduler
    backfill_enabled: false
```

The backfill method follows `EASYScheduler.backfill` as of commit
`6bb91090b8610f0e6f2b456885511a8ce31257d1`. Only the node-demand field changes
from `full_socket_nodes` to `half_socket_nodes`, and allocation changes from
`compact_allocation` to `spread_allocation`. An explicit disabled-mode guard
is added. The original `easy.py` and `spreadonly.py` are unchanged.
The regression tests compare the two method syntax trees after these exact
substitutions to detect accidental policy divergence.

The head job is reserved using running jobs' requested wall-time end estimates.
A later job may use reserved idle nodes if its wall time fits before that
reservation. Otherwise it must fit on nodes outside the reservation.
The implementation deliberately retains the branch's EASY traversal,
reservation selection, and return-value behavior. It is not a rewritten policy.

V2 reuses the original spread setup and FIFO deployment, adds `omit_hosts`
support to node selection for the current allocation API, and defaults to
backfill enabled. Setup rejects unsupported socket sizes, missing measured
spread speeds, and jobs larger than the cluster's spread capacity.

## Verification

From the Elise repository, with the Elise dependencies installed:

```bash
python -m unittest discover -s tests -p test_spreadonly_v2.py -v
```

Tests exercise short jobs on reserved nodes, rejection of long jobs on reserved
nodes, long jobs on non-reserved nodes, disabled backfill, zero backfill depth,
node exclusivity, resource release, and exact correspondence with EASY's logic.

## Companion co-scheduling experiments

The parent research project contains `spread_scheduler/` notebooks (outside
this Elise repository):

- 01 builds measured runtime pools.
- 02 runs FIFO versus V2 with `backfill_enabled: false`; 03 plots these results.
- 04 runs EASY versus V2 with `backfill_enabled: true`; 05 plots these results.

In 02 and 04 set matching lists, for example:

```python
APPS = [605, 613, 618, 619]
PROCS = [256, 512, 1024]
RUN_NAME = "uniform_apps_procs_v2"
```

Each job independently draws uniformly from both lists, with replacement.
The generator creates an Elise JSON entry for every app/process-count pair,
then writes CSV jobs referencing that entry. Changing only a CSV process-count
column would not suffice because Elise's List Generator reads size from JSON.

Only the 1,024-rank JUSUF timings are measured in these source pools. Other
process counts reuse those runtimes unchanged and are explicitly synthetic
resource-size variants, not a scaling prediction. The three 2/4/8-way timing
scenarios all use Elise's two-way spread allocation. Input manifests retain
provenance and identify synthetic process counts.
