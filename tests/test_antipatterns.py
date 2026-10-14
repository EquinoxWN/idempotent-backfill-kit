"""The baseline: plain appends duplicate on rerun and a non-atomic overwrite leaves a gap."""

from __future__ import annotations

import pytest
from helpers import DemoDb

from idempotent_backfill_kit.antipatterns import non_atomic_overwrite, plain_append
from idempotent_backfill_kit.demo import (
    DAILY_ORDER_STATS,
    ORDER_EVENTS,
    check_job,
    compare,
    expected_rows,
)
from idempotent_backfill_kit.runner import FaultPoint, SimulatedCrash, crash_at, run_job


def _crash() -> None:
    """Die between the DELETE and the INSERT."""
    raise SimulatedCrash("after_delete")


def test_plain_append_duplicates_every_row_on_rerun(demo_db: DemoDb) -> None:
    for _ in range(2):
        for p in demo_db.partitions:
            plain_append(demo_db.engine, ORDER_EVENTS, p)
    actual = demo_db.engine.query("SELECT * FROM fct_order_events")
    dup, missing, _, _ = compare(
        "order_events", actual, expected_rows("order_events", demo_db.events)
    )
    assert len(actual) == 2 * len(demo_db.events)
    assert dup == 2 * len(demo_db.events) - len({e.event_id for e in demo_db.events})
    assert missing == 0


def test_dedup_insert_on_the_same_reruns_has_no_duplicates(demo_db: DemoDb) -> None:
    for _ in range(2):
        run_job(demo_db.engine, ORDER_EVENTS, demo_db.partitions)
    result = check_job(demo_db.engine, ORDER_EVENTS, demo_db.events, demo_db.partitions)
    assert result.duplicates == result.defects == 0


def test_non_atomic_overwrite_leaves_a_gap_after_a_crash(demo_db: DemoDb) -> None:
    for p in demo_db.partitions:
        non_atomic_overwrite(demo_db.engine, DAILY_ORDER_STATS, p, lambda: None)
    victim = demo_db.partitions[2]
    with pytest.raises(SimulatedCrash):
        non_atomic_overwrite(demo_db.engine, DAILY_ORDER_STATS, victim, _crash)
    demo_db.reopen()
    actual = demo_db.engine.query("SELECT * FROM agg_daily_order_stats")
    _, missing, _, _ = compare(
        "daily_order_stats", actual, expected_rows("daily_order_stats", demo_db.events)
    )
    lost = len(expected_rows("daily_order_stats", demo_db.events, {victim}))
    assert missing == lost > 0


def test_atomic_overwrite_with_the_same_crash_loses_nothing(demo_db: DemoDb) -> None:
    run_job(demo_db.engine, DAILY_ORDER_STATS, demo_db.partitions)
    victim = demo_db.partitions[2]
    with pytest.raises(SimulatedCrash):
        run_job(
            demo_db.engine,
            DAILY_ORDER_STATS,
            demo_db.partitions,
            fault=crash_at(FaultPoint.AFTER_DELETE, victim),
        )
    demo_db.reopen()
    result = check_job(demo_db.engine, DAILY_ORDER_STATS, demo_db.events, demo_db.partitions)
    assert result.missing == result.defects == 0
    assert result.target_rows == result.expected_rows
