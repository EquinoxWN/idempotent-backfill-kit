"""Two common non-idempotent writes, kept only to measure the defects the patterns remove."""

from __future__ import annotations

from collections.abc import Callable

from .engines import Engine
from .spec import JobSpec, quote


def _cols(spec: JobSpec) -> str:
    """Comma-separated quoted columns."""
    return ", ".join(quote(c) for c in spec.columns)


def plain_append(engine: Engine, spec: JobSpec, partition: str) -> int:
    """INSERT ... SELECT with no key check: every rerun appends the partition again."""
    return engine.dml(
        f"INSERT INTO {quote(spec.target)} ({_cols(spec)}) SELECT {_cols(spec)} "
        f"FROM {quote(spec.source)} WHERE {quote(spec.partition_column)} = ?",
        [partition],
    )


def non_atomic_overwrite(
    engine: Engine, spec: JobSpec, partition: str, after_delete: Callable[[], None]
) -> int:
    """DELETE and INSERT as two autocommitted statements: a crash between them leaves a gap."""
    p = quote(spec.partition_column)
    engine.dml(f"DELETE FROM {quote(spec.target)} WHERE {p} = ?", [partition])
    after_delete()
    return engine.dml(
        f"INSERT INTO {quote(spec.target)} ({_cols(spec)}) SELECT {_cols(spec)} "
        f"FROM {quote(spec.source)} WHERE {p} = ?",
        [partition],
    )
