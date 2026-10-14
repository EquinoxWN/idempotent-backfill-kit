"""Crash mid-run at every fault point, restart, rerun: never a duplicate, never a gap."""

from __future__ import annotations

import subprocess
import sys

import pytest
from helpers import DemoDb

from idempotent_backfill_kit.bench import HARD_KILL_EXIT, fault_points
from idempotent_backfill_kit.demo import JOBS, check_job
from idempotent_backfill_kit.runner import FaultPoint, SimulatedCrash, crash_at, run_job

CASES = [(job, point) for job, spec in JOBS.items() for point in fault_points(spec)]


@pytest.mark.parametrize(("job", "point"), CASES, ids=[f"{j}-{p.value}" for j, p in CASES])
def test_crash_then_rerun_converges(demo_db: DemoDb, job: str, point: FaultPoint) -> None:
    spec = JOBS[job]
    index = 2
    victim = demo_db.partitions[index]
    with pytest.raises(SimulatedCrash):
        run_job(demo_db.engine, spec, demo_db.partitions, fault=crash_at(point, victim))
    demo_db.reopen()  # the process died: whatever was not committed is gone

    after_crash = check_job(demo_db.engine, spec, demo_db.events, demo_db.partitions)
    committed = index + (1 if point is FaultPoint.AFTER_COMMIT else 0)
    assert after_crash.partitions_committed == committed
    assert after_crash.defects == 0, after_crash  # exactly the committed partitions are visible

    run_job(demo_db.engine, spec, demo_db.partitions)
    final = check_job(demo_db.engine, spec, demo_db.events, demo_db.partitions)
    assert (final.defects, final.partitions_not_committed) == (0, 0), final
    assert final.target_rows == final.expected_rows


HARD = [
    ("order_events", FaultPoint.AFTER_LEDGER),
    ("orders", FaultPoint.AFTER_LEDGER),
    ("daily_order_stats", FaultPoint.AFTER_DELETE),
]


@pytest.mark.parametrize(("job", "point"), HARD, ids=[j for j, _ in HARD])
def test_killed_process_leaves_only_committed_partitions(
    demo_db: DemoDb, job: str, point: FaultPoint
) -> None:
    spec = JOBS[job]
    run_job(demo_db.engine, spec, demo_db.partitions[:1])
    demo_db.engine.close()
    victim = demo_db.partitions[3]
    cmd = [
        sys.executable,
        "-m",
        "idempotent_backfill_kit",
        "run",
        "--db",
        str(demo_db.path),
        "--job",
        job,
        "--start",
        demo_db.partitions[0],
        "--end",
        demo_db.partitions[-1],
        "--simulate-crash",
        f"{point.value}@{victim}",
    ]
    done = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
    assert done.returncode == HARD_KILL_EXIT, done.stderr
    demo_db.reopen()

    after_kill = check_job(demo_db.engine, spec, demo_db.events, demo_db.partitions)
    assert after_kill.partitions_committed == 3
    assert after_kill.defects == 0, after_kill
    # Close and open again before the rerun: DuckDB checkpoints the replayed log on close, the
    # sequence that lost index entries in DuckDB 1.5.6 (ADR 0004).
    demo_db.reopen()

    run_job(demo_db.engine, spec, demo_db.partitions)
    final = check_job(demo_db.engine, spec, demo_db.events, demo_db.partitions)
    assert (final.defects, final.partitions_not_committed) == (0, 0), final
