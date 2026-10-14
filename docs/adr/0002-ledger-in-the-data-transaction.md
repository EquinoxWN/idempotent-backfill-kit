# ADR 0002: Keep the run ledger in the warehouse, committed in the same transaction as each partition

- **Status:** Accepted

## Context

The ledger has to answer "which partitions did this job finish, and up to which watermark?" after
any crash. Common places for that state are the scheduler's database, a JSON file next to the
job, or a separate metadata database. Each is a second system: the data commit and the ledger
write cannot be atomic without a two-phase protocol. A crash between them leaves either a
partition that is in the warehouse but not in the ledger (the rerun repeats it, which is only
safe if the write is idempotent anyway) or a ledger entry for a partition whose data was never
committed (a silent gap that no rerun will fill, because the ledger says it is done).

## Decision

The ledger tables (`ibk_runs`, `ibk_partition_commits`) live in the target database. For every
partition the runner opens one transaction, reads the source's row count and watermark range,
writes through the pattern, checks the touched keys, inserts the partition's ledger row and only
then commits. `Ledger.record` refuses to run outside an open transaction. Run status rows
(`running`, `succeeded`, `failed`, `abandoned`) are written in their own short transactions,
because they describe the run, not the data.

## Consequences

- A committed partition and its ledger row become visible together or not at all. The crash
  matrix checks this at five points inside the write on both engines: after a crash, the
  partitions the ledger lists are exactly the partitions whose data is visible (0 mismatches in 26
  crash scenarios and 6 killed processes in `ibk bench`).
- The ledger needs no reconciliation step, and "what is missing" is a query
  (`missing_partitions`), which is what M2's resume-from-watermark builds on.
- The kit needs write access to create two `ibk_` tables in the warehouse, and the ledger cannot
  span several databases. A job that writes to two warehouses would need one ledger per database.
- Run statuses can lag: a killed process leaves its run `running` until the next run of the job
  marks it `abandoned`. Partition commits, not run statuses, are the source of truth.
