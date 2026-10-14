# Contributing

Thanks for helping. This is a Python project; issues and pull requests are welcome.

## Set up and check your change

Needs Python 3.11+. DuckDB installs as a wheel and SQLite ships with Python, so no database
server or Docker is needed. Caches, virtual environments and demo databases stay in git-ignored
folders inside the repo.

```bash
make setup   # install dependencies (exact versions from constraints.txt)
make lint    # ruff, ruff format, mypy --strict
make test    # full test suite on DuckDB and SQLite
make bench   # every pattern through runs, reruns and crashes, plus the anti-pattern baseline
make audit   # known-vulnerability check (as in CI)
```

A pull request is ready when `make lint`, `make test` and `make bench` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- A new write pattern or engine needs the crash matrix in `tests/test_crash.py` to pass for it.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
