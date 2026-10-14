"""The run ledger: which partitions and watermarks each run committed, stored with the data."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from .engines import Engine

RunStatus = Literal["running", "succeeded", "failed", "abandoned"]
MAX_ERROR_CHARS = 500

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS ibk_runs (
        run_id TEXT NOT NULL,
        job TEXT NOT NULL,
        pattern TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'abandoned')),
        partitions_requested BIGINT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        error TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS ibk_partition_commits (
        run_id TEXT NOT NULL,
        job TEXT NOT NULL,
        partition_value TEXT NOT NULL,
        watermark_low TEXT,
        watermark_high TEXT,
        rows_read BIGINT NOT NULL,
        rows_inserted BIGINT NOT NULL,
        rows_updated BIGINT NOT NULL,
        rows_deleted BIGINT NOT NULL,
        committed_at TEXT NOT NULL
    )""",
)
# Unique indexes only where they survive crash recovery; the runner never writes duplicates anyway.
UNIQUE_INDEXES = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ibk_runs_id ON ibk_runs (run_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ibk_commits_id ON ibk_partition_commits (run_id, partition_value)",
)


@dataclass(frozen=True)
class PartitionCommit:
    """One partition a run committed, with its watermark range and row counts."""

    run_id: str
    job: str
    partition: str
    watermark_low: str | None
    watermark_high: str | None
    rows_read: int
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    committed_at: str


@dataclass(frozen=True)
class RunRecord:
    """One run of one job and how it ended."""

    run_id: str
    job: str
    pattern: str
    status: RunStatus
    partitions_requested: int
    started_at: str
    finished_at: str | None
    error: str | None


class Ledger:
    """Reads and writes the ledger tables through the job's own connection."""

    def __init__(self, engine: Engine) -> None:
        """Bind to an engine; call ensure_schema before first use."""
        self.engine = engine

    def ensure_schema(self) -> None:
        """Create the ledger tables if they do not exist."""
        for ddl in SCHEMA:
            self.engine.execute(ddl)
        if self.engine.durable_unique_index:
            for ddl in UNIQUE_INDEXES:
                self.engine.execute(ddl)

    def start_run(self, run_id: str, job: str, pattern: str, partitions: int, now: str) -> None:
        """Record a new running run; earlier runs of the job still marked running are abandoned."""
        self.engine.begin()
        try:
            self.engine.execute(
                "UPDATE ibk_runs SET status = 'abandoned', finished_at = ? "
                "WHERE job = ? AND status = 'running'",
                [now, job],
            )
            self.engine.execute(
                "INSERT INTO ibk_runs (run_id, job, pattern, status, partitions_requested, "
                "started_at) VALUES (?, ?, ?, 'running', ?, ?)",
                [run_id, job, pattern, partitions, now],
            )
        except BaseException:
            self.engine.rollback()
            raise
        self.engine.commit()

    def record(self, commit: PartitionCommit) -> None:
        """Add a partition commit; the caller's transaction also holds the partition's data."""
        if not self.engine.in_transaction:
            raise RuntimeError("a partition commit must share the data's transaction")
        self.engine.execute(
            "INSERT INTO ibk_partition_commits (run_id, job, partition_value, watermark_low, "
            "watermark_high, rows_read, rows_inserted, rows_updated, rows_deleted, committed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                commit.run_id,
                commit.job,
                commit.partition,
                commit.watermark_low,
                commit.watermark_high,
                commit.rows_read,
                commit.rows_inserted,
                commit.rows_updated,
                commit.rows_deleted,
                commit.committed_at,
            ],
        )

    def finish_run(
        self, run_id: str, status: RunStatus, now: str, error: str | None = None
    ) -> None:
        """Mark a run as succeeded or failed (autocommit)."""
        self.engine.execute(
            "UPDATE ibk_runs SET status = ?, finished_at = ?, error = ? WHERE run_id = ?",
            [status, now, None if error is None else error[:MAX_ERROR_CHARS], run_id],
        )

    def runs(self, job: str | None = None) -> list[RunRecord]:
        """Runs in start order, optionally for one job."""
        where, params = ("WHERE job = ?", [job]) if job else ("", [])
        rows = self.engine.query(
            "SELECT run_id, job, pattern, status, partitions_requested, started_at, finished_at, "
            f"error FROM ibk_runs {where} ORDER BY started_at, run_id",
            params,
        )
        return [RunRecord(r[0], r[1], r[2], r[3], int(r[4]), r[5], r[6], r[7]) for r in rows]

    def commits(self, job: str | None = None, run_id: str | None = None) -> list[PartitionCommit]:
        """Partition commits in commit order, filtered by job and/or run."""
        clauses, params = [], []
        if job:
            clauses.append("job = ?")
            params.append(job)
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.engine.query(
            "SELECT run_id, job, partition_value, watermark_low, watermark_high, rows_read, "
            "rows_inserted, rows_updated, rows_deleted, committed_at FROM ibk_partition_commits "
            f"{where} ORDER BY committed_at, partition_value",
            params,
        )
        return [
            PartitionCommit(
                r[0], r[1], r[2], r[3], r[4], int(r[5]), int(r[6]), int(r[7]), int(r[8]), r[9]
            )
            for r in rows
        ]

    def committed_partitions(self, job: str) -> set[str]:
        """Every partition some run of the job has committed."""
        rows = self.engine.query(
            "SELECT DISTINCT partition_value FROM ibk_partition_commits WHERE job = ?", [job]
        )
        return {r[0] for r in rows}

    def missing_partitions(self, job: str, expected: Iterable[str]) -> list[str]:
        """Expected partitions that no run of the job has committed (the gaps)."""
        done = self.committed_partitions(job)
        return sorted(p for p in set(expected) if p not in done)

    def high_watermark(self, job: str) -> str | None:
        """Highest watermark any committed partition of the job reached."""
        value = self.engine.scalar(
            "SELECT max(watermark_high) FROM ibk_partition_commits WHERE job = ?", [job]
        )
        return None if value is None else str(value)
