# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/idempotent-backfill-kit/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** A rerun or crash that leaves duplicate, missing or partially written rows; a ledger
  entry that disagrees with the committed data; SQL injection through table, column or job names,
  partition values or job files; unbounded input (job files, partition ranges, generated sources).
- **Out of scope:** Bugs in DuckDB or SQLite themselves (report those upstream; the DuckDB index
  behaviour this project works around is described in ADR 0004).

## How this repository protects itself

- Table and column names must match a strict identifier pattern and are quoted; every value
  (partition, watermark, row data) is a bound parameter. Partitions must be real calendar dates.
- Job files are size-limited TOML with an exact set of keys and types.
- Every GitHub Action is pinned to a full commit SHA, and workflows run with read-only
  permissions and without persisted credentials. Dependencies install from `constraints.txt`.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs lint, strict type checks, tests, the benchmark and a known-vulnerability check
  (pip-audit) on every push and pull request.
