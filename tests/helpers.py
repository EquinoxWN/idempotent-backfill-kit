"""Shared test helpers: seeded demo databases on either engine."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from idempotent_backfill_kit.demo import (
    SourceEvent,
    create_schema,
    generate_events,
    load_source,
    read_source,
    source_partitions,
)
from idempotent_backfill_kit.engines import Engine, EngineKind, connect

KINDS: tuple[EngineKind, ...] = ("duckdb", "sqlite")
SUFFIX = {"duckdb": ".duckdb", "sqlite": ".sqlite"}


@dataclass
class DemoDb:
    """A seeded demo database file and what was loaded into it."""

    kind: EngineKind
    path: Path
    engine: Engine
    events: list[SourceEvent]
    partitions: list[str]

    def reopen(self) -> None:
        """Close and reopen, as a restarted process would."""
        self.engine.close()
        self.engine = connect(self.kind, str(self.path))

    def refresh(self) -> None:
        """Re-read the source after late data was added."""
        self.events = read_source(self.engine)
        self.partitions = source_partitions(self.engine)


def seeded(
    kind: EngineKind, folder: Path, days: int = 6, per_day: int = 60, seed: int = 3
) -> DemoDb:
    """Create a demo database file with a generated source."""
    path = folder / f"demo{SUFFIX[kind]}"
    engine = connect(kind, str(path))
    create_schema(engine)
    load_source(engine, generate_events("2026-03-01", days, per_day, seed))
    return DemoDb(kind, path, engine, read_source(engine), source_partitions(engine))
