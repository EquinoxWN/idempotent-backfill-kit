"""The ibk command line end to end: seed, run, crash, ledger, verify and bench."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from helpers import SUFFIX

from idempotent_backfill_kit.bench import HARD_KILL_EXIT
from idempotent_backfill_kit.cli import EXIT_DEFECTS, EXIT_USAGE, main
from idempotent_backfill_kit.engines import EngineKind

RANGE = ["--start", "2026-01-01", "--end", "2026-01-04"]


def seed(db: Path) -> None:
    """Seed a small demo source."""
    assert main(["seed", "--db", str(db), "--days", "4", "--events-per-day", "50"]) == 0


def test_seed_run_verify_and_ledger(
    kind: EngineKind, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / f"demo{SUFFIX[kind]}"
    seed(db)
    for job in ("order_events", "orders", "daily_order_stats"):
        assert main(["run", "--db", str(db), "--job", job, *RANGE]) == 0
    capsys.readouterr()
    assert main(["verify", "--db", str(db), "--complete"]) == 0
    checks = json.loads(capsys.readouterr().out)
    assert [c["defects"] for c in checks] == [0, 0, 0]
    assert main(["ledger", "--db", str(db), "--job", "orders"]) == 0
    ledger = json.loads(capsys.readouterr().out)
    assert [r["status"] for r in ledger["runs"]] == ["succeeded"]
    assert len(ledger["partition_commits"]) == 4


def test_usage_errors_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "demo.sqlite"
    seed(db)
    assert main(["seed", "--db", str(db)]) == EXIT_USAGE  # refuses to load a second source
    assert main(["run", "--db", str(db), "--job", "nope", *RANGE]) == EXIT_USAGE
    assert (
        main(
            [
                "run",
                "--db",
                str(db),
                "--job",
                "orders",
                "--start",
                "2026-1-1",
                "--end",
                "2026-01-02",
            ]
        )
        == EXIT_USAGE
    )
    assert main(["run", "--db", str(tmp_path / "x.csv"), "--job", "orders", *RANGE]) == EXIT_USAGE
    bad = [
        "run",
        "--db",
        str(db),
        "--job",
        "orders",
        *RANGE,
        "--simulate-crash",
        "later@2026-01-02",
    ]
    assert main(bad) == EXIT_USAGE
    assert main(["bench", "--engines", "oracle"]) == EXIT_USAGE
    assert "error:" in capsys.readouterr().err


def orders_check(capsys: pytest.CaptureFixture[str], db: Path) -> dict[str, int]:
    """Run verify and return the orders entry."""
    capsys.readouterr()
    assert main(["verify", "--db", str(db)]) == 0
    checks = json.loads(capsys.readouterr().out)
    entry: dict[str, int] = next(c for c in checks if c["job"] == "orders")
    return entry


def test_simulated_crash_exits_abruptly_and_a_rerun_completes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "demo.duckdb"
    seed(db)
    cmd = [
        sys.executable,
        "-m",
        "idempotent_backfill_kit",
        "run",
        "--db",
        str(db),
        "--job",
        "orders",
        *RANGE,
    ]
    killed = subprocess.run(
        [*cmd, "--simulate-crash", "after_write@2026-01-03"], check=False, timeout=300
    )
    assert killed.returncode == HARD_KILL_EXIT
    # A separate process opens and closes the file in between (DuckDB checkpoints here).
    after_kill = orders_check(capsys, db)
    assert (after_kill["defects"], after_kill["partitions_not_committed"]) == (0, 2)
    assert main(["verify", "--db", str(db), "--complete"]) == EXIT_DEFECTS
    assert subprocess.run(cmd, check=False, timeout=300, capture_output=True).returncode == 0
    final = orders_check(capsys, db)
    assert (final["defects"], final["duplicates"], final["partitions_not_committed"]) == (0, 0, 0)
    assert main(["ledger", "--db", str(db), "--job", "orders"]) == 0
    runs = json.loads(capsys.readouterr().out)["runs"]
    assert [r["status"] for r in runs] == ["abandoned", "succeeded"]


def test_custom_jobs_from_a_toml_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "demo.sqlite"
    seed(db)
    jobs = tmp_path / "jobs.toml"
    jobs.write_text(
        "[[job]]\n"
        'name = "events_copy"\npattern = "dedup_insert"\nsource = "src_order_events"\n'
        'target = "fct_order_events"\ncolumns = ["event_id", "order_id", "version", "status", '
        '"amount_cents", "event_time", "event_date"]\npartition_column = "event_date"\n'
        'watermark_column = "event_time"\nkey = ["event_id"]\n',
        encoding="utf-8",
    )
    capsys.readouterr()
    assert main(["run", "--db", str(db), "--jobs", str(jobs), "--job", "events_copy", *RANGE]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["partitions"] == 4
    assert out["inserted"] == 200
    assert out["high_watermark"].startswith("2026-01-04")


def test_bench_reports_zero_defects_and_a_failing_baseline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report = tmp_path / "bench.json"
    args = ["bench", "--workdir", str(tmp_path / "b"), "--engines", "sqlite", "--days", "4"]
    assert main([*args, "--events-per-day", "40", "--json", str(report)]) == 0
    assert "RESULT patterns:" in capsys.readouterr().out
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["pattern_defects"] == 0
    assert len(data["scenarios"]) == 41
    assert data["baseline"][0]["plain_append_rerun"]["duplicates"] > 0
    assert data["baseline"][0]["non_atomic_overwrite_crash"]["missing"] > 0
