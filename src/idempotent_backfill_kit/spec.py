"""Job specifications: source, target, keys, partitions and the write pattern a job uses."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
JOB_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}")
ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
RESERVED_PREFIX = "ibk_"
MAX_COLUMNS = 200
MAX_PARTITIONS = 10_000
MAX_JOB_FILE_BYTES = 1 << 20
JOB_KEYS = frozenset(
    {
        "name",
        "pattern",
        "source",
        "target",
        "columns",
        "partition_column",
        "watermark_column",
        "key",
        "version_column",
    }
)


class SpecError(ValueError):
    """A job specification or argument that cannot be run safely."""


class Pattern(StrEnum):
    """The three idempotent write patterns."""

    MERGE = "merge"
    OVERWRITE = "overwrite"
    DEDUP_INSERT = "dedup_insert"


def check_identifier(name: str, what: str) -> str:
    """Validate a SQL identifier (table or column name) and return it unchanged."""
    if not isinstance(name, str) or not IDENTIFIER.fullmatch(name):
        raise SpecError(f"{what} must match {IDENTIFIER.pattern}, got {name!r}")
    return name


def quote(name: str) -> str:
    """Validate an identifier and double-quote it for SQL."""
    return f'"{check_identifier(name, "identifier")}"'


def parse_partition(value: str) -> str:
    """Validate a daily partition value in strict YYYY-MM-DD form."""
    if not isinstance(value, str) or not ISO_DATE.fullmatch(value):
        raise SpecError(f"partition must be YYYY-MM-DD, got {value!r}")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SpecError(f"partition is not a calendar date: {value!r}") from exc
    return value


def partition_range(start: str, end: str) -> list[str]:
    """Every daily partition from start to end, inclusive."""
    first, last = (
        date.fromisoformat(parse_partition(start)),
        date.fromisoformat(parse_partition(end)),
    )
    if last < first:
        raise SpecError(f"end {end} is before start {start}")
    days = (last - first).days + 1
    if days > MAX_PARTITIONS:
        raise SpecError(f"{days} partitions requested, at most {MAX_PARTITIONS} per run")
    return [(first + timedelta(days=i)).isoformat() for i in range(days)]


def check_partitions(partitions: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Validate a run's partition list: non-empty, bounded, unique, each a date."""
    parts = tuple(parse_partition(p) for p in partitions)
    if not parts:
        raise SpecError("a run needs at least one partition")
    if len(parts) > MAX_PARTITIONS:
        raise SpecError(f"{len(parts)} partitions requested, at most {MAX_PARTITIONS} per run")
    if len(set(parts)) != len(parts):
        raise SpecError("partition list contains duplicates")
    return parts


@dataclass(frozen=True)
class JobSpec:
    """One backfill job: copy a source partition into a target through one pattern."""

    name: str
    pattern: Pattern
    source: str
    target: str
    columns: tuple[str, ...]
    partition_column: str
    watermark_column: str
    key: tuple[str, ...] = ()
    version_column: str | None = None

    def __post_init__(self) -> None:
        """Reject any specification the runner could not execute safely."""
        if not isinstance(self.name, str) or not JOB_NAME.fullmatch(self.name):
            raise SpecError(f"job name must match {JOB_NAME.pattern}, got {self.name!r}")
        if not isinstance(self.pattern, Pattern):
            raise SpecError(f"pattern must be a Pattern, got {self.pattern!r}")
        for table, what in ((self.source, "source"), (self.target, "target")):
            check_identifier(table, what)
            if table.lower().startswith(RESERVED_PREFIX):
                raise SpecError(f"{what} {table!r} uses the reserved prefix {RESERVED_PREFIX!r}")
        if self.source.lower() == self.target.lower():
            raise SpecError("source and target must be different tables")
        if not self.columns or len(self.columns) > MAX_COLUMNS:
            raise SpecError(f"columns must list 1 to {MAX_COLUMNS} names")
        for column in (*self.columns, *self.key, self.partition_column, self.watermark_column):
            check_identifier(column, "column")
        lowered = [c.lower() for c in self.columns]
        if len(set(lowered)) != len(lowered):
            raise SpecError("columns contain duplicates")
        if len({k.lower() for k in self.key}) != len(self.key):
            raise SpecError("key contains duplicates")
        if any(k.lower() not in lowered for k in self.key):
            raise SpecError("every key column must be one of the columns")
        self._check_pattern(lowered)

    def _check_pattern(self, lowered: list[str]) -> None:
        """Pattern-specific rules: keys, version column and partition column."""
        if self.pattern is Pattern.MERGE:
            if not self.key:
                raise SpecError("merge needs a natural key")
            if self.version_column is None:
                raise SpecError("merge needs a version column so newer rows win in any order")
            check_identifier(self.version_column, "version column")
            if self.version_column.lower() not in lowered:
                raise SpecError("the version column must be one of the columns")
            if self.version_column.lower() in {k.lower() for k in self.key}:
                raise SpecError("the version column cannot be part of the key")
            return
        if self.version_column is not None:
            raise SpecError(f"{self.pattern.value} does not use a version column")
        if self.pattern is Pattern.DEDUP_INSERT:
            if not self.key:
                raise SpecError("dedup_insert needs a dedup key")
            return
        if self.key:
            raise SpecError("overwrite replaces whole partitions and takes no key")
        if self.partition_column.lower() not in lowered:
            raise SpecError("overwrite needs the partition column among the target columns")

    @property
    def value_columns(self) -> tuple[str, ...]:
        """Columns that are not part of the key."""
        keys = {k.lower() for k in self.key}
        return tuple(c for c in self.columns if c.lower() not in keys)


def _strings(raw: Any, field: str, job: str) -> tuple[str, ...]:
    """A list of strings from a parsed job file."""
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        raise SpecError(f"job {job!r}: {field} must be a list of strings")
    return tuple(raw)


def _string(raw: Any, field: str, job: str) -> str:
    """One string from a parsed job file."""
    if not isinstance(raw, str):
        raise SpecError(f"job {job!r}: {field} must be a string")
    return raw


def job_from_mapping(raw: dict[str, Any]) -> JobSpec:
    """Build a JobSpec from one [[job]] table, with exact keys."""
    name = str(raw.get("name", "?"))
    unknown = set(raw) - JOB_KEYS
    if unknown:
        raise SpecError(f"job {name!r}: unknown keys {sorted(unknown)}")
    required = {"name", "pattern", "source", "target", "columns"}
    required |= {"partition_column", "watermark_column"}
    missing = required - set(raw)
    if missing:
        raise SpecError(f"job {name!r}: missing keys {sorted(missing)}")
    try:
        pattern = Pattern(_string(raw["pattern"], "pattern", name))
    except ValueError as exc:
        raise SpecError(
            f"job {name!r}: pattern must be one of {[p.value for p in Pattern]}"
        ) from exc
    version = raw.get("version_column")
    return JobSpec(
        name=_string(raw["name"], "name", name),
        pattern=pattern,
        source=_string(raw["source"], "source", name),
        target=_string(raw["target"], "target", name),
        columns=_strings(raw["columns"], "columns", name),
        partition_column=_string(raw["partition_column"], "partition_column", name),
        watermark_column=_string(raw["watermark_column"], "watermark_column", name),
        key=_strings(raw.get("key", []), "key", name),
        version_column=None if version is None else _string(version, "version_column", name),
    )


def load_jobs(path: Path) -> dict[str, JobSpec]:
    """Read job specifications from a TOML file of [[job]] tables."""
    size = path.stat().st_size
    if size > MAX_JOB_FILE_BYTES:
        raise SpecError(f"{path} is {size} bytes, at most {MAX_JOB_FILE_BYTES}")
    try:
        doc = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise SpecError(f"{path}: {exc}") from exc
    if set(doc) != {"job"} or not isinstance(doc["job"], list):
        raise SpecError(f"{path}: expected only [[job]] tables")
    jobs: dict[str, JobSpec] = {}
    for raw in doc["job"]:
        if not isinstance(raw, dict):
            raise SpecError(f"{path}: every job must be a table")
        spec = job_from_mapping(raw)
        if spec.name in jobs:
            raise SpecError(f"{path}: job {spec.name!r} defined twice")
        jobs[spec.name] = spec
    return jobs
