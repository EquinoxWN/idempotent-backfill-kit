"""Minimal reproduction: a DuckDB unique index misses WAL-replayed rows after an unclean exit.

usage: python scripts/duckdb_index_after_unclean_exit.py <folder>
Each scenario uses its own database file in <folder> and three separate processes:
  1. create a table with a unique index, insert two committed rows, exit without closing;
  2. open and close the file (DuckDB replays the write-ahead log and checkpoints);
  3. insert a row whose key already exists, then MERGE a newer version of an existing key.
Observed with DuckDB 1.5.6 on Windows 11 (see docs/results/m1.md): after step 2 the duplicate is
accepted and the MERGE inserts a second row instead of updating. The control scenario, which
checkpoints before exiting, rejects the duplicate.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb

MERGE = (
    "MERGE INTO t USING (SELECT 'b' AS k, 5 AS v) AS s ON t.k = s.k "
    "WHEN MATCHED AND s.v > t.v THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v)"
)


def step(db: str, number: str, checkpoint: bool) -> None:
    """Run one step of the scenario in this process."""
    con = duckdb.connect(db)
    if number == "1":
        con.execute("CREATE TABLE t (k VARCHAR NOT NULL, v BIGINT NOT NULL)")
        con.execute("CREATE UNIQUE INDEX t_k ON t (k)")
        con.execute("INSERT INTO t VALUES ('a', 1), ('b', 1)")
        if checkpoint:
            con.execute("CHECKPOINT")
        os._exit(0)  # no close: the process dies with the rows only in the write-ahead log
    if number == "3":
        try:
            con.execute("INSERT INTO t VALUES ('a', 9)")
            print("  duplicate key 'a': accepted")
        except duckdb.ConstraintException:
            print("  duplicate key 'a': rejected")
        con.execute(MERGE)
        print(
            "  rows after MERGE of ('b', 5):",
            con.execute("SELECT * FROM t ORDER BY k, v").fetchall(),
        )
    con.close()


def main() -> None:
    """Run the unclean-exit scenario and the checkpointed control, each in fresh processes."""
    if len(sys.argv) == 5:
        step(sys.argv[2], sys.argv[3], sys.argv[4] == "1")
        return
    folder = Path(sys.argv[1])
    folder.mkdir(parents=True, exist_ok=True)
    print("duckdb", duckdb.__version__, flush=True)
    for name, checkpoint in (
        ("unclean exit, rows only in the WAL", "0"),
        ("control: CHECKPOINT first", "1"),
    ):
        db = folder / f"repro-{checkpoint}.duckdb"
        for leftover in (db, db.with_name(db.name + ".wal")):
            leftover.unlink(missing_ok=True)
        print(name, flush=True)
        for number in ("1", "2", "3"):
            subprocess.run(  # noqa: S603 - fixed arguments, this interpreter
                [sys.executable, __file__, "step", str(db), number, checkpoint], check=True
            )


if __name__ == "__main__":
    main()
