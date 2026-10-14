"""Both embedded engines: transactions, row counts, bulk loading and the newer-wins upsert."""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers import SUFFIX

from idempotent_backfill_kit.engines import (
    DuckDBEngine,
    Engine,
    EngineError,
    EngineKind,
    connect,
    engine_kind,
    open_path,
)
from idempotent_backfill_kit.spec import SpecError


@pytest.mark.parametrize(
    ("name", "kind"),
    [("w.duckdb", "duckdb"), ("w.sqlite", "sqlite"), ("w.SQLITE3", "sqlite"), ("w.db", "sqlite")],
)
def test_engine_follows_the_file_suffix(name: str, kind: str) -> None:
    assert engine_kind(name) == kind


@pytest.mark.parametrize("name", ["w.csv", "w", "w.duckdb.bak"])
def test_unknown_suffix_is_rejected(name: str) -> None:
    with pytest.raises(SpecError, match="must end in"):
        engine_kind(name)


def test_commit_survives_reopen_and_rollback_does_not(kind: EngineKind, tmp_path: Path) -> None:
    path = tmp_path / f"t{SUFFIX[kind]}"
    eng = open_path(path)
    eng.execute("CREATE TABLE t (k TEXT, v BIGINT)")
    eng.begin()
    eng.execute("INSERT INTO t VALUES ('kept', 1)")
    eng.commit()
    eng.begin()
    eng.execute("INSERT INTO t VALUES ('lost', 2)")
    eng.rollback()
    eng.begin()
    eng.execute("INSERT INTO t VALUES ('open', 3)")
    eng.close()  # an open transaction dies with the connection
    eng = open_path(path)
    assert eng.query("SELECT k FROM t") == [("kept",)]
    eng.close()


def test_nested_begin_is_refused(engine: Engine) -> None:
    engine.begin()
    with pytest.raises(EngineError, match="already open"):
        engine.begin()
    engine.rollback()


def test_dml_reports_changed_rows(engine: Engine) -> None:
    engine.execute("CREATE TABLE t (k TEXT, v BIGINT)")
    assert engine.dml("INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)") == 3
    assert engine.dml("UPDATE t SET v = v + 1 WHERE k <> ?", ["a"]) == 2
    assert engine.dml("DELETE FROM t WHERE v > ?", [2]) == 2
    assert engine.scalar("SELECT count(*) FROM t") == 1


def test_columns_of_tables_and_views(engine: Engine) -> None:
    engine.execute("CREATE TABLE t (K TEXT, v BIGINT)")
    engine.execute("CREATE VIEW w AS SELECT k, v * 2 AS doubled FROM t")
    assert engine.columns("t") == ["k", "v"]
    assert engine.columns("w") == ["k", "doubled"]
    assert engine.table_exists("w")
    assert not engine.table_exists("nope")
    with pytest.raises(EngineError):
        engine.columns("nope")


def test_bulk_insert_round_trips_awkward_text(engine: Engine) -> None:
    engine.execute("CREATE TABLE t (a TEXT, b BIGINT, c TEXT)")
    rows = [
        ('quote " and , comma', 1, "line\nbreak"),
        ("", 2, None),
        ("ünïcode ✓", -9_000_000_000, "tab\there"),
    ]
    engine.begin()
    engine.bulk_insert("t", ("a", "b", "c"), rows)
    engine.commit()
    assert sorted(engine.query("SELECT a, b, c FROM t"), key=lambda r: r[1]) == sorted(
        rows, key=lambda r: r[1]
    )


def test_duckdb_bulk_insert_refuses_lossy_values() -> None:
    eng = DuckDBEngine(":memory:")
    eng.execute("CREATE TABLE t (a TEXT)")
    with pytest.raises(EngineError, match="text and integers"):
        eng.bulk_insert("t", ("a",), [(1.5,)])
    with pytest.raises(EngineError, match="expected 1"):
        eng.bulk_insert("t", ("a",), [("x", "y")])
    eng.close()


def test_upsert_applies_only_newer_versions(kind: EngineKind) -> None:
    eng = connect(kind)
    eng.execute("CREATE TABLE t (k TEXT NOT NULL, v BIGINT NOT NULL, x TEXT)")
    eng.execute("CREATE UNIQUE INDEX t_k ON t (k)")
    eng.execute("CREATE TABLE s (k TEXT, v BIGINT, x TEXT, p TEXT)")
    eng.execute(
        "INSERT INTO s VALUES ('a', 2, 'new', 'd1'), ('b', 1, 'b1', 'd1'), ('a', 1, 'old', 'd2')"
    )
    batch = "SELECT k, v, x FROM s WHERE p = ?"
    sql = eng.upsert_sql("t", ("k", "v", "x"), ("k",), "v", batch)
    assert eng.dml(sql, ["d1"]) == 2
    assert eng.dml(sql, ["d2"]) == 0  # version 1 is older than the stored version 2
    assert eng.dml(sql, ["d1"]) == 0  # same versions again: nothing to do
    assert sorted(eng.query("SELECT k, v, x FROM t")) == [("a", 2, "new"), ("b", 1, "b1")]
    eng.close()


def test_connect_rejects_unknown_kind() -> None:
    with pytest.raises(SpecError, match="unknown engine"):
        connect("postgres")  # type: ignore[arg-type]
