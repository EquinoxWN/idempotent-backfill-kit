"""The three idempotent write patterns, each applied to one source partition."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .engines import Engine
from .spec import JobSpec, Pattern, SpecError, quote

AfterDelete = Callable[[], None]


class BatchError(ValueError):
    """Source rows that would make a rerun depend on processing order."""


class DuplicateKeyError(BatchError):
    """The target would hold two rows for one key after this partition's write."""


class EmptySourceError(BatchError):
    """An overwrite would replace a non-empty target partition with nothing."""


@dataclass(frozen=True)
class WriteCounts:
    """Rows a pattern changed in the target."""

    inserted: int
    updated: int
    deleted: int


@dataclass(frozen=True)
class PartitionStats:
    """Source rows and watermark range of one partition, read inside the write transaction."""

    rows: int
    watermark_low: str | None
    watermark_high: str | None


def version_of(spec: JobSpec) -> str:
    """The merge version column; a spec without one never reaches the merge code."""
    if spec.version_column is None:
        raise SpecError(f"job {spec.name}: merge needs a version column")
    return spec.version_column


def _cols(spec: JobSpec, alias: str = "") -> str:
    """Comma-separated quoted columns, optionally qualified."""
    prefix = f"{alias}." if alias else ""
    return ", ".join(prefix + quote(c) for c in spec.columns)


def distinct_batch(spec: JobSpec) -> str:
    """Source rows of one partition with exact duplicate deliveries collapsed (one ? parameter)."""
    return (
        f"SELECT DISTINCT {_cols(spec)} FROM {quote(spec.source)} "
        f"WHERE {quote(spec.partition_column)} = ?"
    )


def latest_batch(spec: JobSpec) -> str:
    """One row per key: the highest version in the partition (one ? parameter)."""
    keys = ", ".join(quote(k) for k in spec.key)
    return (
        f"SELECT {_cols(spec)} FROM (SELECT {_cols(spec)}, row_number() OVER "
        f"(PARTITION BY {keys} ORDER BY {quote(version_of(spec))} DESC) AS ibk_rn "
        f"FROM ({distinct_batch(spec)}) AS ibk_d) AS ibk_r WHERE ibk_rn = 1"
    )


def _key_match(spec: JobSpec, left: str, right: str) -> str:
    """Join condition on the key columns."""
    return " AND ".join(f"{left}.{quote(k)} = {right}.{quote(k)}" for k in spec.key)


def read_stats(engine: Engine, spec: JobSpec, partition: str) -> PartitionStats:
    """Count source rows and their watermark range for a partition."""
    w = quote(spec.watermark_column)
    row = engine.query(
        f"SELECT count(*), CAST(min({w}) AS TEXT), CAST(max({w}) AS TEXT) "
        f"FROM {quote(spec.source)} WHERE {quote(spec.partition_column)} = ?",
        [partition],
    )[0]
    return PartitionStats(int(row[0]), row[1], row[2])


def check_batch(engine: Engine, spec: JobSpec, partition: str) -> None:
    """Fail fast on NULL keys and on same-key rows whose content disagrees."""
    if spec.pattern is Pattern.OVERWRITE:
        return
    identity = list(spec.key)
    if spec.pattern is Pattern.MERGE:
        identity.append(version_of(spec))
    nulls = " OR ".join(f"{quote(c)} IS NULL" for c in identity)
    found = engine.scalar(
        f"SELECT count(*) FROM {quote(spec.source)} "
        f"WHERE {quote(spec.partition_column)} = ? AND ({nulls})",
        [partition],
    )
    if found:
        raise BatchError(
            f"job {spec.name}, partition {partition}: {found} source rows have a NULL "
            f"{'key or version' if spec.pattern is Pattern.MERGE else 'key'}"
        )
    group = ", ".join(quote(c) for c in identity)
    clashes = engine.scalar(
        f"SELECT count(*) FROM (SELECT {group} FROM ({distinct_batch(spec)}) AS ibk_d "
        f"GROUP BY {group} HAVING count(*) > 1) AS ibk_g",
        [partition],
    )
    if clashes:
        raise BatchError(
            f"job {spec.name}, partition {partition}: {clashes} "
            f"{'key/version pairs' if spec.pattern is Pattern.MERGE else 'keys'} "
            "arrive with different content; the result would depend on processing order"
        )
    _check_against_target(engine, spec, partition)


def _check_against_target(engine: Engine, spec: JobSpec, partition: str) -> None:
    """Reject rows that clash with what the target already holds for the same identity."""
    compare = [c for c in spec.value_columns if c != spec.version_column]
    if not compare:
        return
    differs = " OR ".join(
        engine.null_safe_differs(f"ibk_s.{quote(c)}", f"ibk_t.{quote(c)}") for c in compare
    )
    same = _key_match(spec, "ibk_t", "ibk_s")
    if spec.pattern is Pattern.MERGE:
        v = quote(version_of(spec))
        same += f" AND ibk_t.{v} = ibk_s.{v}"
    clashes = engine.scalar(
        f"SELECT count(*) FROM ({distinct_batch(spec)}) AS ibk_s "
        f"JOIN {quote(spec.target)} AS ibk_t ON {same} WHERE {differs}",
        [partition],
    )
    if clashes:
        raise BatchError(
            f"job {spec.name}, partition {partition}: {clashes} rows disagree with the target "
            "for the same key"
            + (" and version" if spec.pattern is Pattern.MERGE else "")
            + "; a rerun must not change history silently"
        )


def ensure_key_index(engine: Engine, spec: JobSpec) -> None:
    """Unique index on the key where the engine keeps it intact through crash recovery."""
    if not spec.key or not engine.durable_unique_index:
        return
    keys = ", ".join(quote(k) for k in spec.key)
    index = quote(f"ibk_uq_{spec.target}"[:63])
    engine.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {quote(spec.target)} ({keys})")


def check_touched_keys(engine: Engine, spec: JobSpec, partition: str) -> None:
    """After the write, inside the transaction: every key this partition touched has one row."""
    if not spec.key:
        return
    keys = ", ".join(f"ibk_t.{quote(k)}" for k in spec.key)
    dupes = engine.scalar(
        f"SELECT count(*) FROM (SELECT {keys} FROM {quote(spec.target)} AS ibk_t WHERE EXISTS "
        f"(SELECT 1 FROM ({distinct_batch(spec)}) AS ibk_s WHERE {_key_match(spec, 'ibk_t', 'ibk_s')}) "
        f"GROUP BY {keys} HAVING count(*) > 1) AS ibk_g",
        [partition],
    )
    if dupes:
        raise DuplicateKeyError(
            f"job {spec.name}, partition {partition}: {dupes} keys would have more than one row "
            f"in {spec.target}; nothing was committed"
        )


def _count(engine: Engine, spec: JobSpec) -> int:
    """Rows currently in the target."""
    return int(engine.scalar(f"SELECT count(*) FROM {quote(spec.target)}"))


def merge(engine: Engine, spec: JobSpec, partition: str) -> WriteCounts:
    """MERGE on the natural key: insert new keys, update only when the source version is newer."""
    before = _count(engine, spec)
    sql = engine.upsert_sql(
        spec.target, spec.columns, spec.key, version_of(spec), latest_batch(spec)
    )
    changed = engine.dml(sql, [partition])
    inserted = _count(engine, spec) - before
    return WriteCounts(inserted=inserted, updated=changed - inserted, deleted=0)


def dedup_insert(engine: Engine, spec: JobSpec, partition: str) -> WriteCounts:
    """Insert rows whose dedup key the target does not hold yet; never update."""
    sql = (
        f"INSERT INTO {quote(spec.target)} ({_cols(spec)}) SELECT {_cols(spec, 'ibk_s')} "
        f"FROM ({distinct_batch(spec)}) AS ibk_s WHERE NOT EXISTS "
        f"(SELECT 1 FROM {quote(spec.target)} AS ibk_t WHERE {_key_match(spec, 'ibk_t', 'ibk_s')})"
    )
    return WriteCounts(inserted=engine.dml(sql, [partition]), updated=0, deleted=0)


def overwrite(
    engine: Engine,
    spec: JobSpec,
    partition: str,
    stats: PartitionStats,
    after_delete: AfterDelete,
    allow_empty: bool = False,
) -> WriteCounts:
    """Replace the target partition with the source partition (caller holds the transaction)."""
    p = quote(spec.partition_column)
    if stats.rows == 0 and not allow_empty:
        existing = engine.scalar(
            f"SELECT count(*) FROM {quote(spec.target)} WHERE {p} = ?", [partition]
        )
        if existing:
            raise EmptySourceError(
                f"job {spec.name}, partition {partition}: source is empty but the target holds "
                f"{existing} rows; pass allow_empty to really clear it"
            )
    deleted = engine.dml(f"DELETE FROM {quote(spec.target)} WHERE {p} = ?", [partition])
    after_delete()
    inserted = engine.dml(
        f"INSERT INTO {quote(spec.target)} ({_cols(spec)}) "
        f"SELECT {_cols(spec)} FROM {quote(spec.source)} WHERE {p} = ?",
        [partition],
    )
    return WriteCounts(inserted=inserted, updated=0, deleted=deleted)


def check_tables(engine: Engine, spec: JobSpec) -> None:
    """Fail fast when the source or target lacks a column the job needs."""
    source, target = engine.columns(spec.source), engine.columns(spec.target)
    needed = {c.lower() for c in spec.columns}
    needed |= {spec.partition_column.lower(), spec.watermark_column.lower()}
    missing = sorted(needed - set(source))
    if missing:
        raise SpecError(f"job {spec.name}: source {spec.source} lacks columns {missing}")
    missing = sorted({c.lower() for c in spec.columns} - set(target))
    if missing:
        raise SpecError(f"job {spec.name}: target {spec.target} lacks columns {missing}")
