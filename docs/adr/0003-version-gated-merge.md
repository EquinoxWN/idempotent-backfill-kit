# ADR 0003: MERGE needs a version column; newer wins, and conflicting rows are rejected

- **Status:** Accepted

## Context

A backfill processes daily partitions, but an entity (an order) changes over several days. A
MERGE that simply applies "the row from this partition" makes the final state depend on the
order partitions are processed: rerunning January 3rd after January 20th writes the January 3rd
status back over the newer one. The same problem appears inside one partition when an entity
changes twice that day, and when a source sends two different rows that claim to be the same
version.

## Decision

- A MERGE job must name a `version_column` that is not part of the key (the spec is rejected
  otherwise).
- The batch for a partition is the highest version per key among its distinct rows.
- The update branch applies only when the source version is strictly greater than the stored
  one (`WHEN MATCHED AND source.v > target.v` in DuckDB, `ON CONFLICT ... DO UPDATE ... WHERE
  excluded.v > target.v` in SQLite). Equal versions change nothing, so a rerun is a no-op.
- Before writing, the runner rejects a key/version pair that arrives with different content, in
  the batch or compared with the target, and rejects NULL keys or versions. The dedup-insert
  pattern applies the same rule to its key.

## Consequences

- Partition order does not matter: the bench runs every job in a shuffled order and gets the
  oracle's result, and a test runs a later partition first and shows the older version never
  wins. Reruns of unchanged input report 0 inserted and 0 updated rows.
- Sources must provide a monotonic version (a sequence number or an update timestamp with a tie
  breaker). Data without one cannot use MERGE in this kit; it can use partition overwrite.
- Correcting history requires a new version. A source that edits a row in place without bumping
  its version is stopped with an error instead of being applied silently, which is deliberate but
  can block a run until the source is fixed.
