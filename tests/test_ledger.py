"""The run ledger records partitions, watermarks and outcomes in the same transaction as the data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from helpers import DemoDb

from idempotent_backfill_kit.demo import ORDER_EVENTS, ORDERS
from idempotent_backfill_kit.engines import Engine
from idempotent_backfill_kit.ledger import Ledger, PartitionCommit
from idempotent_backfill_kit.patterns import BatchError
from idempotent_backfill_kit.runner import (
    FaultPoint,
    SimulatedCrash,
    crash_at,
    run_job,
)


class Ticks:
    """A clock that moves one second per reading."""

    def __init__(self) -> None:
        """Start at a fixed instant."""
        self.now = datetime(2026, 10, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        """Advance and return the time."""
        self.now += timedelta(seconds=1)
        return self.now


def test_schema_creation_is_repeatable(engine: Engine) -> None:
    ledger = Ledger(engine)
    ledger.ensure_schema()
    ledger.ensure_schema()
    assert ledger.runs() == []


def test_a_run_records_every_partition_with_its_watermarks(demo_db: DemoDb) -> None:
    result = run_job(demo_db.engine, ORDER_EVENTS, demo_db.partitions, clock=Ticks(), run_id="r1")
    ledger = Ledger(demo_db.engine)
    (run,) = ledger.runs("order_events")
    assert (run.run_id, run.status, run.pattern, run.partitions_requested) == (
        "r1",
        "succeeded",
        "dedup_insert",
        len(demo_db.partitions),
    )
    assert run.started_at < (run.finished_at or "")
    commits = ledger.commits("order_events")
    assert commits == list(result.commits)
    for commit in commits:
        mine = [e for e in demo_db.events if e.event_date == commit.partition]
        assert commit.rows_read == len(mine)
        assert commit.watermark_low == min(e.event_time for e in mine)
        assert commit.watermark_high == max(e.event_time for e in mine)
        assert commit.rows_inserted == len({e.event_id for e in mine})
    assert ledger.high_watermark("order_events") == max(e.event_time for e in demo_db.events)
    assert ledger.committed_partitions("order_events") == set(demo_db.partitions)


def test_reruns_are_recorded_as_separate_runs(demo_db: DemoDb) -> None:
    run_job(demo_db.engine, ORDERS, demo_db.partitions, run_id="first")
    run_job(demo_db.engine, ORDERS, demo_db.partitions[:2], run_id="second")
    ledger = Ledger(demo_db.engine)
    assert [r.run_id for r in ledger.runs("orders")] == ["first", "second"]
    second = ledger.commits("orders", run_id="second")
    assert [c.partition for c in second] == demo_db.partitions[:2]
    assert all(c.rows_inserted == c.rows_updated == 0 for c in second)


def test_a_failed_partition_leaves_no_data_and_no_ledger_entry(demo_db: DemoDb) -> None:
    eng = demo_db.engine
    bad = demo_db.partitions[2]
    eng.execute(
        "INSERT INTO src_order_events SELECT event_id, order_id, version, 'tampered', "
        "amount_cents, event_time, event_date FROM src_order_events WHERE event_date = ? LIMIT 1",
        [bad],
    )
    with pytest.raises(BatchError):
        run_job(eng, ORDER_EVENTS, demo_db.partitions, run_id="r1")
    ledger = Ledger(eng)
    (run,) = ledger.runs()
    assert run.status == "failed"
    assert run.error is not None
    assert run.error.startswith("BatchError")
    assert ledger.committed_partitions("order_events") == set(demo_db.partitions[:2])
    assert eng.scalar("SELECT count(*) FROM fct_order_events WHERE event_date = ?", [bad]) == 0
    assert not eng.in_transaction


def test_a_crashed_run_is_marked_abandoned_by_the_next_one(demo_db: DemoDb) -> None:
    victim = demo_db.partitions[1]
    with pytest.raises(SimulatedCrash):
        run_job(
            demo_db.engine,
            ORDERS,
            demo_db.partitions,
            run_id="crashed",
            fault=crash_at(FaultPoint.AFTER_WRITE, victim),
        )
    demo_db.reopen()
    ledger = Ledger(demo_db.engine)
    assert [r.status for r in ledger.runs("orders")] == ["running"]
    run_job(demo_db.engine, ORDERS, demo_db.partitions, run_id="next")
    statuses = {r.run_id: r.status for r in ledger.runs("orders")}
    assert statuses == {"crashed": "abandoned", "next": "succeeded"}
    assert ledger.missing_partitions("orders", demo_db.partitions) == []


def test_missing_partitions_lists_the_gaps(demo_db: DemoDb) -> None:
    done = demo_db.partitions[::2]
    run_job(demo_db.engine, ORDERS, done)
    ledger = Ledger(demo_db.engine)
    assert ledger.missing_partitions("orders", demo_db.partitions) == demo_db.partitions[1::2]
    assert ledger.missing_partitions("other_job", ["2026-01-01"]) == ["2026-01-01"]
    assert ledger.high_watermark("other_job") is None


def test_ledger_entries_cannot_be_written_outside_a_transaction(engine: Engine) -> None:
    ledger = Ledger(engine)
    ledger.ensure_schema()
    commit = PartitionCommit("r", "j", "2026-01-01", None, None, 0, 0, 0, 0, "now")
    with pytest.raises(RuntimeError, match="share the data's transaction"):
        ledger.record(commit)


def test_long_errors_are_truncated(engine: Engine) -> None:
    ledger = Ledger(engine)
    ledger.ensure_schema()
    ledger.start_run("r", "j", "merge", 1, "t0")
    ledger.finish_run("r", "failed", "t1", "x" * 2000)
    (run,) = ledger.runs("j")
    assert run.error is not None
    assert len(run.error) == 500
