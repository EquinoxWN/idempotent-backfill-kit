"""Job specifications, identifiers, partitions and job files are validated before any SQL runs."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from idempotent_backfill_kit.demo import JOBS, ORDER_EVENTS, ORDERS
from idempotent_backfill_kit.spec import (
    MAX_PARTITIONS,
    JobSpec,
    Pattern,
    SpecError,
    check_partitions,
    load_jobs,
    parse_partition,
    partition_range,
    quote,
)


def test_demo_jobs_cover_all_three_patterns() -> None:
    assert {j.pattern for j in JOBS.values()} == set(Pattern)


@pytest.mark.parametrize(
    "name",
    ['t"; DROP TABLE x; --', "a b", "1abc", "", "x" * 64, "tab-le", "t;"],
)
def test_unsafe_identifiers_are_rejected(name: str) -> None:
    with pytest.raises(SpecError):
        quote(name)
    with pytest.raises(SpecError):
        replace(ORDER_EVENTS, target=name)


def test_quote_wraps_valid_identifiers() -> None:
    assert quote("dim_orders") == '"dim_orders"'


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"name": "Bad Name"}, "job name"),
        ({"target": "ibk_runs"}, "reserved prefix"),
        ({"source": "IBK_partition_commits"}, "reserved prefix"),
        ({"target": "SRC_ORDER_EVENTS"}, "different tables"),
        ({"columns": ()}, "columns must list"),
        ({"columns": ("event_id", "EVENT_ID")}, "duplicates"),
        ({"key": ("missing",)}, "key column"),
        ({"key": ()}, "dedup key"),
        ({"version_column": "version"}, "does not use a version"),
    ],
)
def test_dedup_spec_rules(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(SpecError, match=message):
        replace(ORDER_EVENTS, **changes)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"version_column": None}, "version column"),
        ({"key": ()}, "natural key"),
        ({"key": ("order_id", "version")}, "cannot be part of the key"),
        ({"version_column": "updated"}, "one of the columns"),
    ],
)
def test_merge_spec_rules(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(SpecError, match=message):
        replace(ORDERS, **changes)


def test_overwrite_spec_rules() -> None:
    stats = JOBS["daily_order_stats"]
    with pytest.raises(SpecError, match="takes no key"):
        replace(stats, key=("event_date",))
    with pytest.raises(SpecError, match="partition column among"):
        replace(stats, columns=("status", "events"))
    assert stats.value_columns == stats.columns


@pytest.mark.parametrize(
    "value", ["2026-02-30", "2026-1-01", "20260101", "2026-01-01' OR 1=1 --", "", "2026-13-01"]
)
def test_bad_partitions_are_rejected(value: str) -> None:
    with pytest.raises(SpecError):
        parse_partition(value)


def test_partition_range_is_inclusive_and_bounded() -> None:
    assert partition_range("2026-02-27", "2026-03-01") == ["2026-02-27", "2026-02-28", "2026-03-01"]
    with pytest.raises(SpecError, match="before start"):
        partition_range("2026-03-02", "2026-03-01")
    with pytest.raises(SpecError, match="at most"):
        partition_range("1990-01-01", "2026-01-01")
    assert len(partition_range("2000-01-01", "2027-05-18")) == MAX_PARTITIONS


def test_partition_lists_must_be_unique_and_non_empty() -> None:
    assert check_partitions(["2026-01-02", "2026-01-01"]) == ("2026-01-02", "2026-01-01")
    with pytest.raises(SpecError, match="duplicates"):
        check_partitions(["2026-01-01", "2026-01-01"])
    with pytest.raises(SpecError, match="at least one"):
        check_partitions([])


GOOD_FILE = """
[[job]]
name = "orders"
pattern = "merge"
source = "src_order_events"
target = "dim_orders"
columns = ["order_id", "version", "status", "amount_cents", "event_time"]
partition_column = "event_date"
watermark_column = "event_time"
key = ["order_id"]
version_column = "version"

[[job]]
name = "daily"
pattern = "overwrite"
source = "v_daily_order_stats"
target = "agg_daily_order_stats"
columns = ["event_date", "status", "events", "amount_cents", "last_event_time"]
partition_column = "event_date"
watermark_column = "last_event_time"
"""


def test_job_file_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "jobs.toml"
    path.write_text(GOOD_FILE, encoding="utf-8")
    jobs = load_jobs(path)
    assert jobs["orders"] == ORDERS
    assert jobs["daily"].pattern is Pattern.OVERWRITE


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (GOOD_FILE.replace('key = ["order_id"]', 'key = ["order_id"]\nextra = 1'), "unknown keys"),
        (GOOD_FILE.replace('pattern = "merge"', 'pattern = "upsert"'), "pattern must be one of"),
        (GOOD_FILE.replace('columns = ["order_id"', 'columns = [1, "order_id"'), "list of strings"),
        (GOOD_FILE.replace('target = "dim_orders"', "target = 5"), "must be a string"),
        (GOOD_FILE.replace('name = "daily"', 'name = "orders"'), "defined twice"),
        (GOOD_FILE.replace('source = "src_order_events"\n', "", 1), "missing keys"),
        ('title = "x"\n' + GOOD_FILE, "only \\[\\[job\\]\\]"),
        ("[[job]\nname=", "jobs.toml"),
    ],
)
def test_bad_job_files_are_rejected(tmp_path: Path, text: str, message: str) -> None:
    path = tmp_path / "jobs.toml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(SpecError, match=message):
        load_jobs(path)


def test_oversized_job_file_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "jobs.toml"
    path.write_text("#" * ((1 << 20) + 1), encoding="utf-8")
    with pytest.raises(SpecError, match="bytes"):
        load_jobs(path)


def test_spec_requires_a_pattern_enum() -> None:
    with pytest.raises(SpecError, match="Pattern"):
        JobSpec("x", "merge", "a", "b", ("c",), "d", "e")  # type: ignore[arg-type]
