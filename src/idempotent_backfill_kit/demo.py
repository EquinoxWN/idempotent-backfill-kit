"""A seeded order-events source, three demo jobs (one per pattern) and an independent oracle."""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import astuple, dataclass
from datetime import date, timedelta
from typing import Any

from .engines import Engine
from .ledger import Ledger
from .spec import JobSpec, Pattern, SpecError, parse_partition, quote

SOURCE = "src_order_events"
SOURCE_COLUMNS = (
    "event_id",
    "order_id",
    "version",
    "status",
    "amount_cents",
    "event_time",
    "event_date",
)
MAX_DAYS = 3660
MAX_EVENTS_PER_DAY = 100_000
MAX_EVENTS = 2_000_000

SCHEMA = (
    f"""CREATE TABLE IF NOT EXISTS {SOURCE} (
        event_id TEXT NOT NULL, order_id TEXT NOT NULL, version BIGINT NOT NULL,
        status TEXT NOT NULL, amount_cents BIGINT NOT NULL, event_time TEXT NOT NULL,
        event_date TEXT NOT NULL)""",
    f"""CREATE VIEW IF NOT EXISTS v_daily_order_stats AS
        SELECT event_date, status, count(*) AS events, sum(amount_cents) AS amount_cents,
               max(event_time) AS last_event_time
        FROM (SELECT event_id, event_date, status, amount_cents, event_time FROM {SOURCE}
              GROUP BY event_id, event_date, status, amount_cents, event_time) AS ibk_unique_events
        GROUP BY event_date, status""",
    """CREATE TABLE IF NOT EXISTS fct_order_events (
        event_id TEXT NOT NULL, order_id TEXT NOT NULL, version BIGINT NOT NULL,
        status TEXT NOT NULL, amount_cents BIGINT NOT NULL, event_time TEXT NOT NULL,
        event_date TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS dim_orders (
        order_id TEXT NOT NULL, version BIGINT NOT NULL, status TEXT NOT NULL,
        amount_cents BIGINT NOT NULL, event_time TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS agg_daily_order_stats (
        event_date TEXT NOT NULL, status TEXT NOT NULL, events BIGINT NOT NULL,
        amount_cents BIGINT NOT NULL, last_event_time TEXT NOT NULL)""",
)

ORDER_EVENTS = JobSpec(
    name="order_events",
    pattern=Pattern.DEDUP_INSERT,
    source=SOURCE,
    target="fct_order_events",
    columns=SOURCE_COLUMNS,
    partition_column="event_date",
    watermark_column="event_time",
    key=("event_id",),
)
ORDERS = JobSpec(
    name="orders",
    pattern=Pattern.MERGE,
    source=SOURCE,
    target="dim_orders",
    columns=("order_id", "version", "status", "amount_cents", "event_time"),
    partition_column="event_date",
    watermark_column="event_time",
    key=("order_id",),
    version_column="version",
)
DAILY_ORDER_STATS = JobSpec(
    name="daily_order_stats",
    pattern=Pattern.OVERWRITE,
    source="v_daily_order_stats",
    target="agg_daily_order_stats",
    columns=("event_date", "status", "events", "amount_cents", "last_event_time"),
    partition_column="event_date",
    watermark_column="last_event_time",
)
JOBS: dict[str, JobSpec] = {j.name: j for j in (ORDER_EVENTS, ORDERS, DAILY_ORDER_STATS)}
# The column(s) that identify one expected row in each demo target.
ROW_KEYS: dict[str, tuple[int, ...]] = {
    "order_events": (0,),
    "orders": (0,),
    "daily_order_stats": (0, 1),
}

NEXT_STATUS: dict[str, tuple[tuple[str, float], ...]] = {
    "created": (("paid", 0.85), ("cancelled", 0.15)),
    "paid": (("shipped", 0.9), ("cancelled", 0.1)),
    "shipped": (("delivered", 1.0),),
}

Row = tuple[Any, ...]


@dataclass(frozen=True)
class SourceEvent:
    """One order event as it lands in the source table."""

    event_id: str
    order_id: str
    version: int
    status: str
    amount_cents: int
    event_time: str
    event_date: str


@dataclass
class _Order:
    """Generator state for one open order."""

    order_id: str
    version: int
    status: str
    amount_cents: int


def _next_status(rng: random.Random, status: str) -> str:
    """Pick the next status along the order flow."""
    roll, total = rng.random(), 0.0
    options = NEXT_STATUS[status]
    for name, weight in options:
        total += weight
        if roll < total:
            return name
    return options[-1][0]


def generate_events(
    start: str, days: int, events_per_day: int, seed: int, duplicate_rate: float = 0.05
) -> list[SourceEvent]:
    """Seeded order events in arrival order, with exact duplicate deliveries mixed in."""
    if not 1 <= days <= MAX_DAYS:
        raise SpecError(f"days must be 1 to {MAX_DAYS}")
    if not 1 <= events_per_day <= MAX_EVENTS_PER_DAY:
        raise SpecError(f"events per day must be 1 to {MAX_EVENTS_PER_DAY}")
    if days * events_per_day > MAX_EVENTS:
        raise SpecError(f"at most {MAX_EVENTS} events per source")
    if not 0.0 <= duplicate_rate <= 0.5:
        raise SpecError("duplicate rate must be 0 to 0.5")
    rng = random.Random(seed)
    first = date.fromisoformat(parse_partition(start))
    open_orders: list[_Order] = []
    created = 0
    out: list[SourceEvent] = []
    for d in range(days):
        day = (first + timedelta(days=d)).isoformat()
        seconds = sorted(rng.randrange(86_400) for _ in range(events_per_day))
        delivered: list[SourceEvent] = []
        for i, sec in enumerate(seconds):
            if open_orders and rng.random() < 0.6:
                slot = rng.randrange(len(open_orders))
                order = open_orders[slot]
                order.status = _next_status(rng, order.status)
                order.version += 1
                if order.status == "paid":
                    order.amount_cents -= rng.randrange(0, min(500, order.amount_cents))
                if order.status not in NEXT_STATUS:
                    open_orders[slot] = open_orders[-1]
                    open_orders.pop()
            else:
                created += 1
                order = _Order(f"o-{created:07d}", 1, "created", rng.randint(500, 50_000))
                open_orders.append(order)
            event = SourceEvent(
                event_id=f"ev-{d:05d}-{i:06d}",
                order_id=order.order_id,
                version=order.version,
                status=order.status,
                amount_cents=order.amount_cents,
                event_time=f"{day} {sec // 3600:02d}:{sec // 60 % 60:02d}:{sec % 60:02d}",
                event_date=day,
            )
            delivered.append(event)
            if rng.random() < duplicate_rate:
                delivered.append(event)
        rng.shuffle(delivered)
        out.extend(delivered)
    return out


def create_schema(engine: Engine) -> None:
    """Create the source table, the daily view and the three demo targets."""
    for ddl in SCHEMA:
        engine.execute(ddl)
    if engine.kind == "sqlite":
        # SQLite has no zone maps; without this each partition read scans the whole source.
        engine.execute(f"CREATE INDEX IF NOT EXISTS ibk_src_date ON {SOURCE} (event_date)")


def load_source(engine: Engine, events: Sequence[SourceEvent]) -> None:
    """Append events to the source table in one transaction."""
    engine.begin()
    try:
        engine.bulk_insert(SOURCE, SOURCE_COLUMNS, [astuple(e) for e in events])
    except BaseException:
        engine.rollback()
        raise
    engine.commit()


def read_source(engine: Engine) -> list[SourceEvent]:
    """Every source row, duplicates included."""
    cols = ", ".join(quote(c) for c in SOURCE_COLUMNS)
    rows = engine.query(f"SELECT {cols} FROM {SOURCE}")
    return [
        SourceEvent(str(r[0]), str(r[1]), int(r[2]), str(r[3]), int(r[4]), str(r[5]), str(r[6]))
        for r in rows
    ]


def source_partitions(engine: Engine) -> list[str]:
    """Distinct partition values present in the source."""
    return [str(r[0]) for r in engine.query(f"SELECT DISTINCT event_date FROM {SOURCE} ORDER BY 1")]


def expected_rows(
    job: str, events: Iterable[SourceEvent], partitions: set[str] | None = None
) -> dict[Row, Row]:
    """What a correct target holds for the given partitions, computed in plain Python."""
    unique = {e.event_id: e for e in events if partitions is None or e.event_date in partitions}
    if job == "order_events":
        return {(e.event_id,): astuple(e) for e in unique.values()}
    if job == "orders":
        latest: dict[str, SourceEvent] = {}
        for e in unique.values():
            if e.order_id not in latest or e.version > latest[e.order_id].version:
                latest[e.order_id] = e
        return {
            (e.order_id,): (e.order_id, e.version, e.status, e.amount_cents, e.event_time)
            for e in latest.values()
        }
    if job == "daily_order_stats":
        groups: dict[tuple[str, str], list[SourceEvent]] = defaultdict(list)
        for e in unique.values():
            groups[(e.event_date, e.status)].append(e)
        return {
            key: (
                key[0],
                key[1],
                len(es),
                sum(e.amount_cents for e in es),
                max(e.event_time for e in es),
            )
            for key, es in groups.items()
        }
    raise SpecError(f"unknown demo job {job!r}")


@dataclass(frozen=True)
class JobCheck:
    """How a target compares with the oracle for the partitions the ledger says are committed."""

    job: str
    target_rows: int
    expected_rows: int
    duplicates: int
    missing: int
    unexpected: int
    mismatched: int
    partitions_committed: int
    partitions_not_committed: int

    @property
    def defects(self) -> int:
        """Duplicate, missing, unexpected and wrong rows together."""
        return self.duplicates + self.missing + self.unexpected + self.mismatched


def compare(job: str, actual: Sequence[Row], expected: dict[Row, Row]) -> tuple[int, int, int, int]:
    """Duplicates, missing, unexpected and mismatched rows of a target."""
    positions = ROW_KEYS[job]
    seen: dict[Row, Row] = {}
    duplicates = 0
    for row in actual:
        key = tuple(row[i] for i in positions)
        if key in seen:
            duplicates += 1
        else:
            seen[key] = tuple(row)
    missing = sum(1 for k in expected if k not in seen)
    unexpected = sum(1 for k in seen if k not in expected)
    mismatched = sum(1 for k, row in seen.items() if k in expected and row != expected[k])
    return duplicates, missing, unexpected, mismatched


def check_job(
    engine: Engine, spec: JobSpec, events: Sequence[SourceEvent], all_partitions: Sequence[str]
) -> JobCheck:
    """Compare one demo target with the oracle over its committed partitions."""
    ledger = Ledger(engine)
    ledger.ensure_schema()
    committed = ledger.committed_partitions(spec.name)
    expected = expected_rows(spec.name, events, committed)
    cols = ", ".join(quote(c) for c in spec.columns)
    actual = engine.query(f"SELECT {cols} FROM {quote(spec.target)}")
    duplicates, missing, unexpected, mismatched = compare(spec.name, actual, expected)
    return JobCheck(
        job=spec.name,
        target_rows=len(actual),
        expected_rows=len(expected),
        duplicates=duplicates,
        missing=missing,
        unexpected=unexpected,
        mismatched=mismatched,
        partitions_committed=len(committed),
        partitions_not_committed=len(ledger.missing_partitions(spec.name, all_partitions)),
    )


def verify(engine: Engine, jobs: Iterable[JobSpec] = JOBS.values()) -> list[JobCheck]:
    """Check every demo target against the oracle and the ledger."""
    events = read_source(engine)
    partitions = sorted({e.event_date for e in events})
    return [check_job(engine, spec, events, partitions) for spec in jobs]


def reset_job(engine: Engine, spec: JobSpec) -> None:
    """Empty a demo target and forget its ledger history (benchmarks and tests only)."""
    Ledger(engine).ensure_schema()
    engine.begin()
    try:
        engine.execute(f"DELETE FROM {quote(spec.target)}")
        engine.execute("DELETE FROM ibk_partition_commits WHERE job = ?", [spec.name])
        engine.execute("DELETE FROM ibk_runs WHERE job = ?", [spec.name])
    except BaseException:
        engine.rollback()
        raise
    engine.commit()
