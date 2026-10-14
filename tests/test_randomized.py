"""Seeded random histories of runs, reruns, crashes and late data: the targets always match the ledger."""

from __future__ import annotations

import random
from dataclasses import replace
from pathlib import Path

import pytest
from helpers import DemoDb, seeded

from idempotent_backfill_kit.demo import JOBS, SourceEvent, check_job, load_source
from idempotent_backfill_kit.engines import EngineKind
from idempotent_backfill_kit.ledger import Ledger
from idempotent_backfill_kit.runner import FaultPoint, SimulatedCrash, crash_at, run_job

SEEDS = [11, 23, 37, 41]


def late_events(db: DemoDb, rng: random.Random, step: int) -> list[SourceEvent]:
    """New orders and a redelivered old event, landing in an already-processed partition."""
    day = rng.choice(db.partitions)
    fresh = [
        SourceEvent(
            f"late-{step}-{i}",
            f"o-late-{step}-{i}",
            1,
            "created",
            1000 + i,
            f"{day} 23:59:{i:02d}",
            day,
        )
        for i in range(rng.randint(1, 4))
    ]
    return [*fresh, replace(rng.choice(db.events))]


def assert_consistent(db: DemoDb, complete: bool = False) -> None:
    """Every target equals the oracle for exactly the partitions its ledger has committed."""
    for spec in JOBS.values():
        result = check_job(db.engine, spec, db.events, db.partitions)
        assert result.defects == 0, result
        if complete:
            assert result.partitions_not_committed == 0, result


@pytest.mark.parametrize("seed", SEEDS)
def test_random_history_never_duplicates_or_loses_rows(
    kind: EngineKind, tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    db = seeded(kind, tmp_path, days=5, per_day=40, seed=seed)
    try:
        for step in range(8):
            action = rng.choice(["subset", "crash", "late", "full"])
            spec = rng.choice(list(JOBS.values()))
            parts = rng.sample(db.partitions, rng.randint(1, len(db.partitions)))
            if action == "subset":
                run_job(db.engine, spec, parts)
            elif action == "crash":
                points = [p for p in FaultPoint if p is not FaultPoint.AFTER_DELETE]
                hook = crash_at(rng.choice(points), rng.choice(parts))
                try:
                    run_job(db.engine, spec, parts, fault=hook)
                except SimulatedCrash:
                    db.reopen()
            elif action == "late":
                arrivals = late_events(db, rng, step)
                load_source(db.engine, arrivals)
                db.refresh()
                touched = sorted({e.event_date for e in arrivals})
                for job in JOBS.values():  # a committed partition is only current after a rerun
                    if set(touched) & _committed(db, job.name):
                        run_job(db.engine, job, touched)
            else:
                run_job(db.engine, spec, db.partitions)
            assert_consistent(db)
        for spec in JOBS.values():
            run_job(db.engine, spec, db.partitions)
        assert_consistent(db, complete=True)
    finally:
        db.engine.close()


def _committed(db: DemoDb, job: str) -> set[str]:
    """Partitions the ledger says the job has committed."""
    ledger = Ledger(db.engine)
    ledger.ensure_schema()
    return ledger.committed_partitions(job)


def test_late_data_is_visible_as_a_gap_until_the_partition_is_rerun(demo_db: DemoDb) -> None:
    for spec in JOBS.values():
        run_job(demo_db.engine, spec, demo_db.partitions)
    day = demo_db.partitions[1]
    load_source(
        demo_db.engine,
        [SourceEvent("late-1", "o-late-1", 1, "created", 777, f"{day} 23:59:59", day)],
    )
    demo_db.refresh()
    stale = {
        s.job: s
        for s in (
            check_job(demo_db.engine, j, demo_db.events, demo_db.partitions) for j in JOBS.values()
        )
    }
    assert stale["order_events"].missing == 1
    assert stale["orders"].missing == 1
    assert stale["daily_order_stats"].mismatched == 1
    for spec in JOBS.values():
        run_job(demo_db.engine, spec, [day])
    for spec in JOBS.values():
        result = check_job(demo_db.engine, spec, demo_db.events, demo_db.partitions)
        assert (result.defects, result.partitions_not_committed) == (0, 0), result
