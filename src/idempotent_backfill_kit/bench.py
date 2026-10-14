"""The M1 measurement: every pattern on both engines through runs, reruns and crashes, plus a baseline."""

from __future__ import annotations

import contextlib
import platform
import random
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from . import __version__
from .antipatterns import non_atomic_overwrite, plain_append
from .demo import (
    DAILY_ORDER_STATS,
    JOBS,
    ORDER_EVENTS,
    JobCheck,
    SourceEvent,
    check_job,
    compare,
    create_schema,
    expected_rows,
    generate_events,
    load_source,
    read_source,
    reset_job,
    source_partitions,
)
from .engines import Engine, EngineKind, connect
from .runner import FaultPoint, SimulatedCrash, crash_at, run_job
from .spec import JobSpec, Pattern, quote

HARD_KILL_EXIT = 70
SUFFIX: dict[EngineKind, str] = {"duckdb": ".duckdb", "sqlite": ".sqlite"}


@dataclass
class Scenario:
    """One measured scenario and the defects found after it."""

    engine: str
    job: str
    scenario: str
    defects: int
    partitions_committed: int
    partitions_not_committed: int
    detail: dict[str, Any] = field(default_factory=dict)


def fault_points(spec: JobSpec) -> list[FaultPoint]:
    """Fault points that exist for a pattern."""
    points = list(FaultPoint)
    if spec.pattern is not Pattern.OVERWRITE:
        points.remove(FaultPoint.AFTER_DELETE)
    return points


class Bench:
    """Runs every scenario for one engine against one database file."""

    def __init__(
        self,
        kind: EngineKind,
        db: Path,
        events: list[SourceEvent],
        partitions: list[str],
        seed: int,
    ) -> None:
        """Remember the database and its (unchanging) source; open it."""
        self.kind, self.db, self.events, self.partitions = kind, db, events, partitions
        self.rng = random.Random(seed)
        self.engine: Engine = connect(kind, str(db))
        self.results: list[Scenario] = []

    def reopen(self) -> None:
        """Close and reopen the database, as a restarted process would."""
        self.engine.close()
        self.engine = connect(self.kind, str(self.db))

    def check(
        self, spec: JobSpec, scenario: str, expect_committed: int | None = None, **detail: Any
    ) -> JobCheck:
        """Compare the target with the oracle and record the scenario."""
        result = check_job(self.engine, spec, self.events, self.partitions)
        if expect_committed is None:
            expect_committed = len(self.partitions)
        wrong_ledger = int(result.partitions_committed != expect_committed)
        self.results.append(
            Scenario(
                self.kind,
                spec.name,
                scenario,
                result.defects + wrong_ledger,
                result.partitions_committed,
                result.partitions_not_committed,
                {"target_rows": result.target_rows, **detail},
            )
        )
        return result

    def first_and_rerun(self, spec: JobSpec) -> None:
        """A full run, then the same run again: the second must change nothing it should not."""
        reset_job(self.engine, spec)
        started = time.perf_counter()
        first = run_job(self.engine, spec, self.partitions)
        seconds = time.perf_counter() - started
        rows = sum(c.rows_read for c in first.commits)
        self.check(
            spec,
            "first run",
            seconds=round(seconds, 3),
            rows_read=rows,
            rows_per_second=round(rows / seconds) if seconds else None,
            **asdict(first.totals),
        )
        started = time.perf_counter()
        again = run_job(self.engine, spec, self.partitions)
        self.check(
            spec, "rerun", seconds=round(time.perf_counter() - started, 3), **asdict(again.totals)
        )

    def crashes(self, spec: JobSpec) -> None:
        """Crash at every fault point of a random middle partition, restart, check, rerun, check."""
        for point in fault_points(spec):
            reset_job(self.engine, spec)
            index = self.rng.randrange(1, len(self.partitions) - 1)
            victim = self.partitions[index]
            with contextlib.suppress(SimulatedCrash):
                run_job(self.engine, spec, self.partitions, fault=crash_at(point, victim))
            self.reopen()
            committed = index + (1 if point is FaultPoint.AFTER_COMMIT else 0)
            self.check(spec, f"crash {point.value}", committed, partition=victim)
            run_job(self.engine, spec, self.partitions)
            self.check(spec, f"rerun after crash {point.value}")

    def hard_kill(self, spec: JobSpec) -> None:
        """A separate process exits abruptly before committing a partition; then rerun."""
        reset_job(self.engine, spec)
        self.engine.close()
        index = len(self.partitions) // 2
        victim = self.partitions[index]
        cmd = [
            sys.executable,
            "-m",
            "idempotent_backfill_kit",
            "run",
            "--db",
            str(self.db),
            "--job",
            spec.name,
            "--start",
            self.partitions[0],
            "--end",
            self.partitions[-1],
            "--simulate-crash",
            f"{FaultPoint.AFTER_LEDGER.value}@{victim}",
        ]
        code = subprocess.run(cmd, capture_output=True, check=False, timeout=600).returncode  # noqa: S603
        self.engine = connect(self.kind, str(self.db))
        if code != HARD_KILL_EXIT:
            raise RuntimeError(f"the killed run exited with {code}, expected {HARD_KILL_EXIT}")
        self.check(spec, "process killed after_ledger", index, exit_code=code, partition=victim)
        self.reopen()  # a checkpoint of the replayed log in between, as a separate verify would do
        run_job(self.engine, spec, self.partitions)
        self.check(spec, "rerun after kill")

    def shuffled(self, spec: JobSpec) -> None:
        """Partitions processed in random order give the same target."""
        reset_job(self.engine, spec)
        order = list(self.partitions)
        self.rng.shuffle(order)
        run_job(self.engine, spec, order)
        self.check(spec, "shuffled order")

    def baseline(self) -> dict[str, Any]:
        """The two anti-patterns on copies of the demo targets: what goes wrong without the patterns."""
        events = self.events
        append = _copy_spec(ORDER_EVENTS, "ap_plain_append")
        swap = _copy_spec(DAILY_ORDER_STATS, "ap_non_atomic_overwrite")
        for spec in (append, swap):
            self.engine.execute(f"DROP TABLE IF EXISTS {quote(spec.target)}")
            self.engine.execute(
                f"CREATE TABLE {quote(spec.target)} AS SELECT * FROM "
                f"{quote(JOBS[spec.name].target)} WHERE 1 = 0"
            )
        for _ in range(2):
            for p in self.partitions:
                plain_append(self.engine, append, p)
        appended = self.engine.query(f"SELECT * FROM {quote(append.target)}")
        dup = compare(append.name, appended, expected_rows(append.name, events))
        for p in self.partitions:
            non_atomic_overwrite(self.engine, swap, p, lambda: None)
        victim = self.partitions[len(self.partitions) // 2]
        with contextlib.suppress(SimulatedCrash):
            non_atomic_overwrite(self.engine, swap, victim, _raise_crash)
        actual = self.engine.query(f"SELECT * FROM {quote(swap.target)}")
        gap = compare(swap.name, actual, expected_rows(swap.name, events))
        return {
            "engine": self.kind,
            "plain_append_rerun": {"rows": len(appended), "duplicates": dup[0], "missing": dup[1]},
            "non_atomic_overwrite_crash": {"partition": victim, "missing": gap[1]},
        }


def _raise_crash() -> None:
    """Crash between the DELETE and the INSERT."""
    raise SimulatedCrash("after_delete")


def _copy_spec(spec: JobSpec, target: str) -> JobSpec:
    """The same job writing to another table."""
    return JobSpec(
        name=spec.name,
        pattern=spec.pattern,
        source=spec.source,
        target=target,
        columns=spec.columns,
        partition_column=spec.partition_column,
        watermark_column=spec.watermark_column,
        key=spec.key,
        version_column=spec.version_column,
    )


def run_bench(
    workdir: Path,
    kinds: tuple[EngineKind, ...] = ("duckdb", "sqlite"),
    days: int = 30,
    events_per_day: int = 1000,
    seed: int = 7,
) -> dict[str, Any]:
    """Seed a fresh database per engine and run every scenario for every demo job."""
    workdir.mkdir(parents=True, exist_ok=True)
    events = generate_events("2026-01-01", days, events_per_day, seed)
    scenarios: list[Scenario] = []
    baselines: list[dict[str, Any]] = []
    for kind in kinds:
        db = workdir / f"bench{SUFFIX[kind]}"
        for leftover in (db, db.with_name(db.name + ".wal"), db.with_name(db.name + "-journal")):
            leftover.unlink(missing_ok=True)
        setup = connect(kind, str(db))
        create_schema(setup)
        load_source(setup, events)
        partitions = source_partitions(setup)
        stored = read_source(setup)
        setup.close()
        bench = Bench(kind, db, stored, partitions, seed)
        try:
            for spec in JOBS.values():
                bench.first_and_rerun(spec)
                bench.crashes(spec)
                bench.hard_kill(spec)
                bench.shuffled(spec)
            baselines.append(bench.baseline())
        finally:
            bench.engine.close()
        scenarios.extend(bench.results)
    return {
        "kit_version": __version__,
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "source": {
            "days": days,
            "events_per_day": events_per_day,
            "seed": seed,
            "rows": len(events),
            "unique_events": len({e.event_id for e in events}),
        },
        "scenarios": [asdict(s) for s in scenarios],
        "pattern_defects": sum(s.defects for s in scenarios),
        "baseline": baselines,
    }
