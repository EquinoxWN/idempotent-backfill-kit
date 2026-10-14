"""Fixtures: every engine-dependent test runs on DuckDB and on SQLite."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers import KINDS, DemoDb, seeded

from idempotent_backfill_kit.engines import Engine, EngineKind, connect


@pytest.fixture(params=KINDS)
def kind(request: pytest.FixtureRequest) -> EngineKind:
    """Engine kind under test."""
    value: EngineKind = request.param
    return value


@pytest.fixture
def engine(kind: EngineKind) -> Iterator[Engine]:
    """An in-memory engine of the kind under test."""
    eng = connect(kind)
    yield eng
    eng.close()


@pytest.fixture
def demo_db(kind: EngineKind, tmp_path: Path) -> Iterator[DemoDb]:
    """A seeded demo database file of the kind under test."""
    db = seeded(kind, tmp_path)
    yield db
    db.engine.close()
