"""Each write pattern on both engines: reruns change nothing, and unsafe input fails fast."""

from __future__ import annotations

import pytest

from idempotent_backfill_kit.engines import Engine, EngineError, connect
from idempotent_backfill_kit.ledger import Ledger
from idempotent_backfill_kit.patterns import BatchError, DuplicateKeyError, EmptySourceError
from idempotent_backfill_kit.runner import run_job
from idempotent_backfill_kit.spec import JobSpec, Pattern, SpecError

DEDUP = JobSpec(
    name="events",
    pattern=Pattern.DEDUP_INSERT,
    source="src",
    target="events",
    columns=("k", "v", "payload", "day", "ts"),
    partition_column="day",
    watermark_column="ts",
    key=("k",),
)
MERGE = JobSpec(
    name="entities",
    pattern=Pattern.MERGE,
    source="src",
    target="entities",
    columns=("k", "v", "payload", "ts"),
    partition_column="day",
    watermark_column="ts",
    key=("k",),
    version_column="v",
)
OVERWRITE = JobSpec(
    name="daily",
    pattern=Pattern.OVERWRITE,
    source="src",
    target="daily",
    columns=("k", "v", "payload", "day", "ts"),
    partition_column="day",
    watermark_column="ts",
)
D1, D2 = "2026-05-01", "2026-05-02"


def setup(engine: Engine, rows: list[tuple[object, ...]]) -> None:
    """Source and three targets with the test rows."""
    engine.execute("CREATE TABLE src (k TEXT, v BIGINT, payload TEXT, day TEXT, ts TEXT)")
    engine.execute("CREATE TABLE events (k TEXT, v BIGINT, payload TEXT, day TEXT, ts TEXT)")
    engine.execute("CREATE TABLE entities (k TEXT, v BIGINT, payload TEXT, ts TEXT)")
    engine.execute("CREATE TABLE daily (k TEXT, v BIGINT, payload TEXT, day TEXT, ts TEXT)")
    add(engine, rows)


def add(engine: Engine, rows: list[tuple[object, ...]]) -> None:
    """Append source rows."""
    for row in rows:
        engine.execute("INSERT INTO src VALUES (?, ?, ?, ?, ?)", list(row))


def rows(engine: Engine, table: str) -> list[tuple[object, ...]]:
    """Every target row, sorted."""
    return sorted(engine.query(f"SELECT * FROM {table}"), key=repr)


def test_dedup_insert_collapses_redeliveries_and_reruns(engine: Engine) -> None:
    setup(
        engine,
        [
            ("a", 1, "x", D1, "t1"),
            ("a", 1, "x", D1, "t1"),  # the same event delivered twice
            ("b", 1, "y", D1, "t2"),
            ("c", 1, "z", D2, "t3"),
        ],
    )
    first = run_job(engine, DEDUP, [D1, D2])
    assert first.totals.inserted == 3
    again = run_job(engine, DEDUP, [D1, D2])
    assert (again.totals.inserted, again.totals.updated, again.totals.deleted) == (0, 0, 0)
    assert [r[0] for r in rows(engine, "events")] == ["a", "b", "c"]


def test_dedup_insert_rejects_conflicting_content_in_a_batch(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1"), ("a", 1, "DIFFERENT", D1, "t1")])
    with pytest.raises(BatchError, match="different content"):
        run_job(engine, DEDUP, [D1])
    assert rows(engine, "events") == []


def test_dedup_insert_rejects_rows_that_rewrite_history(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1")])
    run_job(engine, DEDUP, [D1])
    engine.execute("UPDATE src SET payload = 'edited'")
    with pytest.raises(BatchError, match="disagree with the target"):
        run_job(engine, DEDUP, [D1])
    assert rows(engine, "events") == [("a", 1, "x", D1, "t1")]


@pytest.mark.parametrize("spec", [DEDUP, MERGE], ids=["dedup", "merge"])
def test_null_keys_are_rejected(engine: Engine, spec: JobSpec) -> None:
    setup(engine, [(None, 1, "x", D1, "t1")])
    with pytest.raises(BatchError, match="NULL"):
        run_job(engine, spec, [D1])


def test_merge_never_lets_an_older_version_win(engine: Engine) -> None:
    setup(engine, [("a", 1, "old", D1, "t1"), ("a", 3, "new", D2, "t9"), ("b", 1, "b", D1, "t2")])
    later_first = run_job(engine, MERGE, [D2, D1])
    assert later_first.commits[1].rows_updated == 0  # D1 holds only an older version of a
    assert rows(engine, "entities") == [("a", 3, "new", "t9"), ("b", 1, "b", "t2")]
    rerun = run_job(engine, MERGE, [D1, D2])
    assert (rerun.totals.inserted, rerun.totals.updated) == (0, 0)


def test_merge_takes_the_highest_version_within_a_partition(engine: Engine) -> None:
    setup(engine, [("a", 1, "v1", D1, "t1"), ("a", 2, "v2", D1, "t2"), ("a", 2, "v2", D1, "t2")])
    result = run_job(engine, MERGE, [D1])
    assert result.totals.inserted == 1
    assert rows(engine, "entities") == [("a", 2, "v2", "t2")]


def test_merge_counts_inserts_and_updates(engine: Engine) -> None:
    setup(engine, [("a", 1, "v1", D1, "t1"), ("a", 2, "v2", D2, "t2"), ("b", 1, "b", D2, "t3")])
    result = run_job(engine, MERGE, [D1, D2])
    assert [(c.rows_inserted, c.rows_updated) for c in result.commits] == [(1, 0), (1, 1)]


def test_merge_rejects_a_version_reused_with_different_content(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1"), ("a", 1, "y", D2, "t1")])
    run_job(engine, MERGE, [D1])
    with pytest.raises(BatchError, match="and version"):
        run_job(engine, MERGE, [D2])
    assert rows(engine, "entities") == [("a", 1, "x", "t1")]


def test_merge_rejects_a_version_reused_inside_one_batch(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1"), ("a", 1, "y", D1, "t1")])
    with pytest.raises(BatchError, match="key/version pairs"):
        run_job(engine, MERGE, [D1])


def test_overwrite_replaces_only_its_partition(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1"), ("b", 1, "y", D2, "t2")])
    run_job(engine, OVERWRITE, [D1, D2])
    engine.execute("DELETE FROM src WHERE k = 'a'")
    add(engine, [("a2", 1, "fixed", D1, "t5")])
    rerun = run_job(engine, OVERWRITE, [D1])
    assert (rerun.totals.deleted, rerun.totals.inserted) == (1, 1)
    assert rows(engine, "daily") == [("a2", 1, "fixed", D1, "t5"), ("b", 1, "y", D2, "t2")]


def test_overwrite_refuses_to_empty_a_partition_unless_asked(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1")])
    run_job(engine, OVERWRITE, [D1])
    engine.execute("DELETE FROM src")
    with pytest.raises(EmptySourceError, match="source is empty"):
        run_job(engine, OVERWRITE, [D1])
    assert len(rows(engine, "daily")) == 1
    run_job(engine, OVERWRITE, [D1], allow_empty=True)
    assert rows(engine, "daily") == []


def test_overwrite_of_a_new_empty_partition_is_allowed(engine: Engine) -> None:
    setup(engine, [])
    result = run_job(engine, OVERWRITE, [D1])
    assert result.commits[0].rows_read == 0


@pytest.mark.parametrize("spec", [DEDUP, MERGE], ids=["dedup", "merge"])
def test_sqlite_key_index_turns_a_stray_duplicate_into_an_error(spec: JobSpec) -> None:
    engine = connect("sqlite")
    setup(engine, [("a", 1, "x", D1, "t1")])
    run_job(engine, spec, [D1])
    cols = ", ".join(spec.columns)
    with pytest.raises(EngineError):
        engine.execute(f"INSERT INTO {spec.target} ({cols}) SELECT {cols} FROM {spec.target}")
    engine.close()


def test_duckdb_targets_get_no_art_index() -> None:
    engine = connect("duckdb")
    setup(engine, [("a", 1, "x", D1, "t1")])
    run_job(engine, DEDUP, [D1])
    run_job(engine, MERGE, [D1])
    assert engine.scalar("SELECT count(*) FROM duckdb_indexes()") == 0
    engine.close()


@pytest.mark.parametrize("spec", [DEDUP, MERGE], ids=["dedup", "merge"])
def test_a_target_already_holding_duplicates_fails_without_committing(
    engine: Engine, spec: JobSpec
) -> None:
    setup(engine, [("a", 2, "x", D1, "t1")])
    cols = ", ".join(spec.columns)
    marks = ", ".join("?" for _ in spec.columns)
    stale = {"k": "a", "v": 2, "payload": "x", "day": D1, "ts": "t1"}
    for _ in range(2):
        engine.execute(
            f"INSERT INTO {spec.target} ({cols}) VALUES ({marks})", [stale[c] for c in spec.columns]
        )
    # SQLite refuses to build the unique index; DuckDB is caught by the post-write key check.
    with pytest.raises(EngineError if engine.durable_unique_index else DuplicateKeyError):
        run_job(engine, spec, [D1])
    ledger = Ledger(engine)
    ledger.ensure_schema()
    assert ledger.committed_partitions(spec.name) == set()


def test_missing_columns_are_reported_before_any_write(engine: Engine) -> None:
    setup(engine, [("a", 1, "x", D1, "t1")])
    engine.execute("CREATE TABLE narrow (k TEXT)")
    with pytest.raises(SpecError, match="target narrow lacks"):
        run_job(engine, JobSpec(**{**DEDUP.__dict__, "target": "narrow"}), [D1])
    bad_source = JobSpec(**{**DEDUP.__dict__, "watermark_column": "nope"})
    with pytest.raises(SpecError, match="source src lacks"):
        run_job(engine, bad_source, [D1])
