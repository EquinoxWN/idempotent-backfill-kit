.PHONY: setup lint test demo bench repro audit clean ci

PY ?= python

setup:
	$(PY) -m pip install --upgrade pip && $(PY) -m pip install -c constraints.txt -e ".[dev]"

# ruff, ruff format and mypy --strict.
lint:
	$(PY) -m ruff check . && $(PY) -m ruff format --check . && $(PY) -m mypy

# Every pattern on DuckDB and SQLite: reruns, crashes at each fault point, killed processes,
# random histories with late data, and the anti-pattern baseline.
test:
	$(PY) -m pytest -q -rs

# Seed a demo warehouse, run the three jobs, crash one run, rerun it and verify.
demo:
	rm -f .tmp/demo/demo.duckdb .tmp/demo/demo.duckdb.wal && mkdir -p .tmp/demo
	ibk seed --db .tmp/demo/demo.duckdb
	ibk run --db .tmp/demo/demo.duckdb --job order_events --start 2026-01-01 --end 2026-01-30
	ibk run --db .tmp/demo/demo.duckdb --job daily_order_stats --start 2026-01-01 --end 2026-01-30
	-ibk run --db .tmp/demo/demo.duckdb --job orders --start 2026-01-01 --end 2026-01-30 --simulate-crash after_ledger@2026-01-16
	ibk run --db .tmp/demo/demo.duckdb --job orders --start 2026-01-01 --end 2026-01-30
	ibk verify --db .tmp/demo/demo.duckdb --complete

# The M1 numbers in docs/results/m1.md: 82 scenarios on 31,476 source rows per engine.
bench:
	ibk bench --json .tmp/bench.json

# The DuckDB index behaviour behind ADR 0004, in three separate processes.
repro:
	$(PY) scripts/duckdb_index_after_unclean_exit.py .tmp/repro-index

# Known vulnerabilities in the installed Python dependencies.
audit:
	$(PY) -m pip_audit --skip-editable --cache-dir .tmp/pip-audit

clean:
	rm -rf .tmp/demo .tmp/bench .tmp/bench.json .tmp/repro-index .tmp/pytest

ci: setup lint test
