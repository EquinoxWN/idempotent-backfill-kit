"""Command line: seed a demo source, run a job, read the ledger, verify, and benchmark."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .bench import HARD_KILL_EXIT, run_bench
from .demo import JOBS, create_schema, generate_events, load_source, verify
from .engines import EngineError, EngineKind, open_path
from .ledger import Ledger
from .patterns import BatchError
from .runner import FaultHook, FaultPoint, run_job
from .spec import JobSpec, SpecError, load_jobs, parse_partition, partition_range

EXIT_DEFECTS = 1
EXIT_USAGE = 2


def _crash_hook(value: str) -> FaultHook:
    """Parse POINT@PARTITION into a hook that ends the process abruptly there."""
    point_text, _, partition = value.partition("@")
    try:
        point = FaultPoint(point_text)
    except ValueError as exc:
        raise SpecError(f"crash point must be one of {[p.value for p in FaultPoint]}") from exc
    partition = parse_partition(partition)

    def hook(at: FaultPoint, part: str) -> None:
        """Exit without cleanup, like a killed process."""
        if at is point and part == partition:
            sys.stdout.flush()
            os._exit(HARD_KILL_EXIT)

    return hook


def _jobs(path: str | None) -> dict[str, JobSpec]:
    """Demo jobs, or the jobs of a TOML file."""
    return load_jobs(Path(path)) if path else dict(JOBS)


def _print(data: Any) -> None:
    """Pretty JSON on stdout."""
    print(json.dumps(data, indent=2, default=str))


def cmd_seed(args: argparse.Namespace) -> int:
    """Create the demo schema and load a seeded source."""
    engine = open_path(args.db)
    try:
        create_schema(engine)
        if engine.scalar("SELECT count(*) FROM src_order_events"):
            raise SpecError(f"{args.db} already holds source rows; use a new file")
        events = generate_events(
            args.start, args.days, args.events_per_day, args.seed, args.duplicate_rate
        )
        load_source(engine, events)
    finally:
        engine.close()
    _print(
        {
            "db": args.db,
            "rows": len(events),
            "unique_events": len({e.event_id for e in events}),
            "partitions": args.days,
        }
    )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Run one job over a partition range."""
    jobs = _jobs(args.jobs)
    if args.job not in jobs:
        raise SpecError(f"unknown job {args.job!r}; known: {sorted(jobs)}")
    hook = _crash_hook(args.simulate_crash) if args.simulate_crash else None
    engine = open_path(args.db)
    try:
        result = run_job(
            engine,
            jobs[args.job],
            partition_range(args.start, args.end),
            fault=hook,
            allow_empty=args.allow_empty,
        )
        high = Ledger(engine).high_watermark(args.job)
    finally:
        engine.close()
    _print(
        {
            "run_id": result.run_id,
            "job": result.job,
            "partitions": len(result.commits),
            **asdict(result.totals),
            "high_watermark": high,
        }
    )
    return 0


def cmd_ledger(args: argparse.Namespace) -> int:
    """Print runs and partition commits."""
    engine = open_path(args.db)
    try:
        ledger = Ledger(engine)
        ledger.ensure_schema()
        runs = [asdict(r) for r in ledger.runs(args.job)]
        commits = [asdict(c) for c in ledger.commits(args.job)]
    finally:
        engine.close()
    _print({"runs": runs, "partition_commits": commits})
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Compare demo targets with the oracle; exit 1 on any defect."""
    engine = open_path(args.db)
    try:
        checks = verify(engine)
    finally:
        engine.close()
    _print([{**asdict(c), "defects": c.defects} for c in checks])
    bad = any(c.defects for c in checks)
    if args.complete:
        bad = bad or any(c.partitions_not_committed for c in checks)
    return EXIT_DEFECTS if bad else 0


def _summary(report: dict[str, Any]) -> str:
    """A human-readable table of the benchmark."""
    lines = [
        f"{'engine':<7} {'job':<18} {'scenario':<26} {'defects':>7} {'committed':>9} {'rows':>7}  detail"
    ]
    for s in report["scenarios"]:
        detail = {k: v for k, v in s["detail"].items() if k != "target_rows"}
        lines.append(
            f"{s['engine']:<7} {s['job']:<18} {s['scenario']:<26} {s['defects']:>7} "
            f"{s['partitions_committed']:>9} {s['detail']['target_rows']:>7}  "
            + " ".join(f"{k}={v}" for k, v in detail.items())
        )
    lines.extend(
        f"baseline {b['engine']}: plain append run twice -> "
        f"{b['plain_append_rerun']['duplicates']} duplicate rows; non-atomic overwrite crash -> "
        f"{b['non_atomic_overwrite_crash']['missing']} missing rows"
        for b in report["baseline"]
    )
    scenarios = len(report["scenarios"])
    dups = sum(b["plain_append_rerun"]["duplicates"] for b in report["baseline"])
    gaps = sum(b["non_atomic_overwrite_crash"]["missing"] for b in report["baseline"])
    lines.append(
        f"RESULT patterns: {scenarios} scenarios, {report['pattern_defects']} defects; "
        f"anti-patterns: {dups} duplicate rows, {gaps} missing rows"
    )
    return "\n".join(lines)


def cmd_bench(args: argparse.Namespace) -> int:
    """Run the M1 benchmark and print a table (and optionally JSON)."""
    kinds: list[EngineKind] = []
    for name in args.engines.split(","):
        if name not in ("duckdb", "sqlite"):
            raise SpecError(f"unknown engine {name!r}")
        kinds.append("duckdb" if name == "duckdb" else "sqlite")
    report = run_bench(Path(args.workdir), tuple(kinds), args.days, args.events_per_day, args.seed)
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(_summary(report))
    baseline_shows_defects = all(
        b["plain_append_rerun"]["duplicates"] and b["non_atomic_overwrite_crash"]["missing"]
        for b in report["baseline"]
    )
    return 0 if report["pattern_defects"] == 0 and baseline_shows_defects else EXIT_DEFECTS


def parser() -> argparse.ArgumentParser:
    """Argument parser with one sub-command per action."""
    p = argparse.ArgumentParser(prog="ibk", description="Idempotent backfills with a run ledger.")
    sub = p.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed", help="create the demo schema and a seeded order-events source")
    seed.add_argument("--db", required=True, help="database file (.duckdb, .sqlite, .sqlite3, .db)")
    seed.add_argument("--start", default="2026-01-01")
    seed.add_argument("--days", type=int, default=30)
    seed.add_argument("--events-per-day", type=int, default=1000)
    seed.add_argument("--seed", type=int, default=7)
    seed.add_argument("--duplicate-rate", type=float, default=0.05)
    seed.set_defaults(func=cmd_seed)

    run = sub.add_parser("run", help="run a job over a daily partition range")
    run.add_argument("--db", required=True)
    run.add_argument(
        "--job", required=True, help=f"demo job ({', '.join(JOBS)}) or one from --jobs"
    )
    run.add_argument("--jobs", help="TOML file of [[job]] tables")
    run.add_argument("--start", required=True, help="first partition, YYYY-MM-DD")
    run.add_argument("--end", required=True, help="last partition, YYYY-MM-DD (inclusive)")
    run.add_argument("--allow-empty", action="store_true", help="let overwrite clear a partition")
    run.add_argument(
        "--simulate-crash",
        metavar="POINT@PARTITION",
        help=f"exit with code {HARD_KILL_EXIT} at that point, without cleanup (crash drills)",
    )
    run.set_defaults(func=cmd_run)

    led = sub.add_parser("ledger", help="print runs and committed partitions")
    led.add_argument("--db", required=True)
    led.add_argument("--job")
    led.set_defaults(func=cmd_ledger)

    ver = sub.add_parser("verify", help="compare demo targets with an independent oracle")
    ver.add_argument("--db", required=True)
    ver.add_argument("--complete", action="store_true", help="also fail on uncommitted partitions")
    ver.set_defaults(func=cmd_verify)

    ben = sub.add_parser("bench", help="every pattern on both engines: runs, reruns and crashes")
    ben.add_argument("--workdir", default=".tmp/bench", help="bench.* files here are replaced")
    ben.add_argument("--engines", default="duckdb,sqlite")
    ben.add_argument("--days", type=int, default=30)
    ben.add_argument("--events-per-day", type=int, default=1000)
    ben.add_argument("--seed", type=int, default=7)
    ben.add_argument("--json", help="also write the full report to this file")
    ben.set_defaults(func=cmd_bench)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; returns the exit code."""
    args = parser().parse_args(argv)
    try:
        code: int = args.func(args)
    except (SpecError, BatchError, EngineError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return code
