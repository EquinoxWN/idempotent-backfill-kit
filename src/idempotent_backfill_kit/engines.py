"""Embedded warehouse engines (DuckDB and SQLite) behind one small interface."""

from __future__ import annotations

import sqlite3
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import duckdb

from .spec import SpecError, check_identifier, quote

EngineKind = Literal["duckdb", "sqlite"]
SUFFIXES: dict[str, EngineKind] = {
    ".duckdb": "duckdb",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".db": "sqlite",
}
Params = Sequence[Any]


class EngineError(RuntimeError):
    """A database error, wrapped so callers need not import each driver."""


class Engine(ABC):
    """One open connection with explicit transaction control."""

    kind: EngineKind
    # Whether a unique index stays complete through crash recovery (see ADR 0004).
    durable_unique_index: bool

    def __init__(self) -> None:
        """Start outside any transaction."""
        self.in_transaction = False

    @abstractmethod
    def _run(self, sql: str, params: Params) -> Any:
        """Execute one statement and return the driver's cursor-like object."""

    @abstractmethod
    def _affected(self, cursor: Any) -> int:
        """Rows changed by the last data-changing statement."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection; an open transaction is discarded."""

    @abstractmethod
    def null_safe_differs(self, left: str, right: str) -> str:
        """SQL that is true when two values differ, treating NULLs as equal."""

    @abstractmethod
    def upsert_sql(
        self, target: str, columns: Sequence[str], key: Sequence[str], version: str, batch: str
    ) -> str:
        """Insert-or-update-if-newer SQL for a batch query with one row per key."""

    begin_sql = "BEGIN TRANSACTION"

    def execute(self, sql: str, params: Params = ()) -> None:
        """Run a statement and ignore its result."""
        self._run(sql, params)

    def query(self, sql: str, params: Params = ()) -> list[tuple[Any, ...]]:
        """Run a query and return every row."""
        return list(self._run(sql, params).fetchall())

    def scalar(self, sql: str, params: Params = ()) -> Any:
        """Run a query and return the first column of its first row."""
        row = self._run(sql, params).fetchone()
        return None if row is None else row[0]

    def dml(self, sql: str, params: Params = ()) -> int:
        """Run an INSERT, UPDATE, DELETE or MERGE and return the rows it changed."""
        return self._affected(self._run(sql, params))

    def begin(self) -> None:
        """Open an explicit transaction."""
        if self.in_transaction:
            raise EngineError("a transaction is already open")
        self.execute(self.begin_sql)
        self.in_transaction = True

    def commit(self) -> None:
        """Commit the open transaction."""
        self.execute("COMMIT")
        self.in_transaction = False

    def rollback(self) -> None:
        """Roll back the open transaction."""
        self.in_transaction = False
        self.execute("ROLLBACK")

    @abstractmethod
    def bulk_insert(self, table: str, columns: Sequence[str], rows: Sequence[Params]) -> None:
        """Append many rows inside the caller's transaction."""

    def columns(self, table: str) -> list[str]:
        """Column names of a table or view, lower-cased."""
        cursor = self._run(f"SELECT * FROM {quote(table)} LIMIT 0", ())
        return [str(d[0]).lower() for d in cursor.description]

    def table_exists(self, table: str) -> bool:
        """Whether a table or view of that name exists."""
        try:
            self.columns(table)
        except EngineError:
            return False
        return True


class DuckDBEngine(Engine):
    """DuckDB file or in-memory database; MERGE INTO for upserts."""

    kind: EngineKind = "duckdb"
    # DuckDB 1.5.6 lost index entries for WAL-replayed rows after an unclean exit (ADR 0004).
    durable_unique_index = False

    def __init__(self, path: str, threads: int = 2, memory_limit: str = "512MB") -> None:
        """Open the database with bounded threads and memory."""
        super().__init__()
        try:
            self.conn = duckdb.connect(
                path, config={"threads": threads, "memory_limit": memory_limit}
            )
        except duckdb.Error as exc:
            raise EngineError(str(exc)) from exc

    def _run(self, sql: str, params: Params) -> Any:
        """Execute through DuckDB, wrapping its errors."""
        try:
            return self.conn.execute(sql, list(params))
        except duckdb.Error as exc:
            raise EngineError(str(exc)) from exc

    def _affected(self, cursor: Any) -> int:
        """DuckDB returns the changed-row count as a one-row result."""
        row = cursor.fetchone()
        return 0 if row is None else int(row[0])

    def close(self) -> None:
        """Close DuckDB; uncommitted work is discarded."""
        self.conn.close()

    def bulk_insert(self, table: str, columns: Sequence[str], rows: Sequence[Params]) -> None:
        """Stage rows as a private CSV file: binding many Python values one by one is slow."""
        cols = ", ".join(quote(c) for c in columns)
        spec = (
            "{" + ", ".join(f"'{check_identifier(c, 'column')}': 'VARCHAR'" for c in columns) + "}"
        )
        with tempfile.TemporaryDirectory(prefix="ibk-") as folder:
            path = Path(folder) / "rows.csv"
            with path.open("w", encoding="utf-8", newline="") as out:
                for row in rows:
                    if len(row) != len(columns):
                        raise EngineError(f"row has {len(row)} values, expected {len(columns)}")
                    out.write(",".join(_csv_field(v) for v in row) + "\n")
            self.execute(
                f"INSERT INTO {quote(table)} ({cols}) SELECT {cols} FROM read_csv(?, header = false, "
                f"quote = '\"', escape = '\"', allow_quoted_nulls = false, columns = {spec})",
                [str(path)],
            )

    def null_safe_differs(self, left: str, right: str) -> str:
        """DuckDB has IS DISTINCT FROM."""
        return f"{left} IS DISTINCT FROM {right}"

    def upsert_sql(
        self, target: str, columns: Sequence[str], key: Sequence[str], version: str, batch: str
    ) -> str:
        """MERGE INTO that inserts new keys and updates rows only when the source is newer."""
        keys = {k.lower() for k in key}
        on = " AND ".join(f"ibk_t.{quote(k)} = ibk_s.{quote(k)}" for k in key)
        sets = ", ".join(f"{quote(c)} = ibk_s.{quote(c)}" for c in columns if c.lower() not in keys)
        cols = ", ".join(quote(c) for c in columns)
        vals = ", ".join(f"ibk_s.{quote(c)}" for c in columns)
        return (
            f"MERGE INTO {quote(target)} AS ibk_t USING ({batch}) AS ibk_s ON {on} "
            f"WHEN MATCHED AND ibk_s.{quote(version)} > ibk_t.{quote(version)} THEN UPDATE SET {sets} "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
        )


class SQLiteEngine(Engine):
    """SQLite file or in-memory database; INSERT ... ON CONFLICT for upserts."""

    kind: EngineKind = "sqlite"
    durable_unique_index = True
    begin_sql = "BEGIN IMMEDIATE"

    def __init__(self, path: str) -> None:
        """Open in autocommit mode so transactions are explicit, with full durability."""
        super().__init__()
        try:
            self.conn = sqlite3.connect(path, isolation_level=None, timeout=10.0)
            self.conn.execute("PRAGMA synchronous = FULL")
        except sqlite3.Error as exc:
            raise EngineError(str(exc)) from exc

    def _run(self, sql: str, params: Params) -> Any:
        """Execute through sqlite3, wrapping its errors."""
        try:
            return self.conn.execute(sql, tuple(params))
        except sqlite3.Error as exc:
            raise EngineError(str(exc)) from exc

    def _affected(self, cursor: Any) -> int:
        """sqlite3 reports changed rows on the cursor."""
        return int(cursor.rowcount)

    def close(self) -> None:
        """Close SQLite; uncommitted work is discarded."""
        self.conn.close()

    def bulk_insert(self, table: str, columns: Sequence[str], rows: Sequence[Params]) -> None:
        """executemany is fast in SQLite."""
        cols = ", ".join(quote(c) for c in columns)
        marks = ", ".join("?" for _ in columns)
        try:
            self.conn.executemany(
                f"INSERT INTO {quote(table)} ({cols}) VALUES ({marks})", [tuple(r) for r in rows]
            )
        except sqlite3.Error as exc:
            raise EngineError(str(exc)) from exc

    def null_safe_differs(self, left: str, right: str) -> str:
        """SQLite's IS NOT is a null-safe inequality."""
        return f"{left} IS NOT {right}"

    def upsert_sql(
        self, target: str, columns: Sequence[str], key: Sequence[str], version: str, batch: str
    ) -> str:
        """INSERT ... ON CONFLICT DO UPDATE that applies only newer versions."""
        keys = {k.lower() for k in key}
        cols = ", ".join(quote(c) for c in columns)
        sets = ", ".join(
            f"{quote(c)} = excluded.{quote(c)}" for c in columns if c.lower() not in keys
        )
        conflict = ", ".join(quote(k) for k in key)
        return (
            f"INSERT INTO {quote(target)} ({cols}) SELECT {cols} FROM ({batch}) AS ibk_s WHERE true "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {sets} "
            f"WHERE excluded.{quote(version)} > {quote(target)}.{quote(version)}"
        )


def _csv_field(value: Any) -> str:
    """One CSV field: NULL unquoted and empty, everything else quoted."""
    if value is None:
        return ""
    if isinstance(value, bool | float):
        raise EngineError(f"bulk_insert supports text and integers, got {type(value).__name__}")
    return '"' + str(value).replace('"', '""') + '"'


def engine_kind(path: str | Path) -> EngineKind:
    """Pick the engine from the database file's suffix."""
    suffix = Path(path).suffix.lower()
    if suffix not in SUFFIXES:
        raise SpecError(f"database file must end in one of {sorted(SUFFIXES)}, got {str(path)!r}")
    return SUFFIXES[suffix]


def connect(kind: EngineKind, path: str = ":memory:") -> Engine:
    """Open an engine of the given kind."""
    if kind == "duckdb":
        return DuckDBEngine(path)
    if kind == "sqlite":
        return SQLiteEngine(path)
    raise SpecError(f"unknown engine {kind!r}")


def open_path(path: str | Path) -> Engine:
    """Open a database file, choosing the engine from its suffix."""
    return connect(engine_kind(path), str(path))
