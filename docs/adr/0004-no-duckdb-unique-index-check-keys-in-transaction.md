# ADR 0004: No unique indexes on DuckDB; check touched keys inside the transaction on every engine

- **Status:** Accepted

## Context

The first implementation created a unique index on the key of every MERGE and dedup-insert
target, and primary keys on the ledger tables, as a backstop: a bug that tried to write a
duplicate would hit a constraint error instead of committing it.

The CLI test that kills a run and then opens the database from a separate process before the
rerun failed on DuckDB: after the rerun, `dim_orders` held 120 rows for 78 orders, 42 of them
duplicates. A three-process reproduction (`scripts/duckdb_index_after_unclean_exit.py`,
`make repro`) isolates it in DuckDB 1.5.6 on Windows 11:

1. create a table with a unique index and insert two committed rows, then exit without closing
   (the rows are only in the write-ahead log);
2. open and close the file (DuckDB replays the log and checkpoints);
3. insert a row with an existing key: **accepted**; `MERGE` a newer version of an existing key:
   **a second row is inserted** instead of an update.

With a `CHECKPOINT` before the exit, the duplicate is rejected and the `MERGE` updates. Without
any index, `MERGE` matches the existing row correctly. No open transaction is needed to trigger
it; any unclean exit with committed rows still in the log does. A search found no upstream
report of the same symptom (checked 2026-10-08).

## Decision

- Unique indexes are created only on engines where they survive crash recovery: SQLite
  (`durable_unique_index = True`). DuckDB targets and ledger tables get no index
  (`durable_unique_index = False`); DuckDB's `MERGE` then uses a join and matches correctly.
- On every engine, after a partition's write and before its ledger row, the runner counts the
  target rows of every key the partition touched and raises `DuplicateKeyError` if any key has
  more than one. The transaction rolls back, so a duplicate can never be committed through the
  kit, whatever state an index is in.

## Consequences

- With the decision applied, the same kill, separate open and rerun sequence ends with 78 rows
  and 0 duplicates on DuckDB; `tests/test_crash.py` and `tests/test_cli.py` now close and reopen
  the file between the kill and the rerun. With indexes switched back on, the post-write check
  alone stops the rerun with `DuplicateKeyError` (25 keys in the test) instead of committing
  duplicates; both layers were checked.
- On DuckDB, a user-defined primary key on a target is still exposed to the behaviour; the
  post-write check turns it into a failed run rather than silent duplicates.
- The check costs a scan of the touched keys per partition, and DuckDB `MERGE` without an index
  joins against the whole target. At the M1 sizes (30,000 rows) a 30-partition run takes about
  one second; larger targets are measured in M2.
- To revisit when DuckDB changes: run `make repro` on a new release (CI runs it on Linux too); if
  the unclean-exit scenario rejects the duplicate, the DuckDB flag can return to `True`.
