"""Run a job over partitions: one transaction per partition holds both the data and its ledger entry."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from .engines import Engine
from .ledger import Ledger, PartitionCommit
from .patterns import (
    PartitionStats,
    WriteCounts,
    check_batch,
    check_tables,
    check_touched_keys,
    dedup_insert,
    ensure_key_index,
    merge,
    overwrite,
    read_stats,
)
from .spec import JobSpec, Pattern, check_partitions


class FaultPoint(StrEnum):
    """Places where a test or demo can stop a run, as a crash would."""

    BEFORE_WRITE = "before_write"
    AFTER_DELETE = "after_delete"
    AFTER_WRITE = "after_write"
    AFTER_LEDGER = "after_ledger"
    AFTER_COMMIT = "after_commit"


FaultHook = Callable[[FaultPoint, str], None]
Clock = Callable[[], datetime]


class SimulatedCrash(BaseException):
    """Raised by fault hooks; like a killed process, it skips the runner's error handling."""


def crash_at(point: FaultPoint, partition: str) -> FaultHook:
    """A fault hook that raises SimulatedCrash at one point of one partition."""

    def hook(at: FaultPoint, part: str) -> None:
        """Raise when the run reaches the chosen point."""
        if at is point and part == partition:
            raise SimulatedCrash(f"{point.value}@{partition}")

    return hook


def utc_now() -> datetime:
    """The current time in UTC."""
    return datetime.now(UTC)


def _iso(clock: Clock) -> str:
    """Clock reading as an ISO-8601 string with microseconds."""
    return clock().isoformat(timespec="microseconds")


def _no_fault(point: FaultPoint, partition: str) -> None:
    """Default hook: never interrupts."""


@dataclass(frozen=True)
class RunResult:
    """What one run committed."""

    run_id: str
    job: str
    commits: tuple[PartitionCommit, ...]

    @property
    def totals(self) -> WriteCounts:
        """Rows inserted, updated and deleted over all partitions."""
        return WriteCounts(
            inserted=sum(c.rows_inserted for c in self.commits),
            updated=sum(c.rows_updated for c in self.commits),
            deleted=sum(c.rows_deleted for c in self.commits),
        )


def prepare(engine: Engine, spec: JobSpec) -> Ledger:
    """Check the tables, create the ledger and the key index; safe to repeat."""
    check_tables(engine, spec)
    ledger = Ledger(engine)
    ledger.ensure_schema()
    ensure_key_index(engine, spec)
    return ledger


def _write(
    engine: Engine,
    spec: JobSpec,
    partition: str,
    stats: PartitionStats,
    fault: FaultHook,
    allow_empty: bool,
) -> WriteCounts:
    """Apply the job's pattern to one partition."""
    check_batch(engine, spec, partition)
    if spec.pattern is Pattern.MERGE:
        counts = merge(engine, spec, partition)
    elif spec.pattern is Pattern.DEDUP_INSERT:
        counts = dedup_insert(engine, spec, partition)
    else:
        return overwrite(
            engine,
            spec,
            partition,
            stats,
            lambda: fault(FaultPoint.AFTER_DELETE, partition),
            allow_empty,
        )
    check_touched_keys(engine, spec, partition)
    return counts


def run_job(
    engine: Engine,
    spec: JobSpec,
    partitions: Sequence[str],
    *,
    fault: FaultHook | None = None,
    clock: Clock = utc_now,
    run_id: str | None = None,
    allow_empty: bool = False,
) -> RunResult:
    """Write each partition and its ledger entry in one transaction, in the order given."""
    parts = check_partitions(list(partitions))
    hook = fault or _no_fault
    ledger = prepare(engine, spec)
    rid = run_id or uuid.uuid4().hex
    ledger.start_run(rid, spec.name, spec.pattern.value, len(parts), _iso(clock))
    commits: list[PartitionCommit] = []
    try:
        for partition in parts:
            engine.begin()
            hook(FaultPoint.BEFORE_WRITE, partition)
            stats = read_stats(engine, spec, partition)
            counts = _write(engine, spec, partition, stats, hook, allow_empty)
            hook(FaultPoint.AFTER_WRITE, partition)
            commit = PartitionCommit(
                run_id=rid,
                job=spec.name,
                partition=partition,
                watermark_low=stats.watermark_low,
                watermark_high=stats.watermark_high,
                rows_read=stats.rows,
                rows_inserted=counts.inserted,
                rows_updated=counts.updated,
                rows_deleted=counts.deleted,
                committed_at=_iso(clock),
            )
            ledger.record(commit)
            hook(FaultPoint.AFTER_LEDGER, partition)
            engine.commit()
            commits.append(commit)
            hook(FaultPoint.AFTER_COMMIT, partition)
    except Exception as exc:
        if engine.in_transaction:
            engine.rollback()
        ledger.finish_run(rid, "failed", _iso(clock), f"{type(exc).__name__}: {exc}")
        raise
    ledger.finish_run(rid, "succeeded", _iso(clock))
    return RunResult(run_id=rid, job=spec.name, commits=tuple(commits))
