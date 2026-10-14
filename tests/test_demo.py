"""The seeded source generator and the independent oracle the other tests rely on."""

from __future__ import annotations

from collections import defaultdict
from itertools import pairwise

import pytest

from idempotent_backfill_kit.demo import (
    NEXT_STATUS,
    SourceEvent,
    compare,
    expected_rows,
    generate_events,
)
from idempotent_backfill_kit.spec import SpecError


def test_same_seed_same_source_and_different_seed_differs() -> None:
    a = generate_events("2026-01-01", 3, 200, seed=5)
    assert a == generate_events("2026-01-01", 3, 200, seed=5)
    assert a != generate_events("2026-01-01", 3, 200, seed=6)


def test_duplicate_deliveries_are_exact_copies_near_the_requested_rate() -> None:
    events = generate_events("2026-01-01", 10, 1000, seed=1, duplicate_rate=0.05)
    unique = {e.event_id: e for e in events}
    assert len(unique) == 10_000
    assert 0.04 < (len(events) - len(unique)) / len(unique) < 0.06
    assert all(e == unique[e.event_id] for e in events)
    assert len(generate_events("2026-01-01", 2, 100, seed=1, duplicate_rate=0.0)) == 200


def test_order_versions_rise_with_time_and_follow_the_status_flow() -> None:
    events = {e.event_id: e for e in generate_events("2026-01-01", 5, 400, seed=2)}.values()
    by_order: dict[str, list[SourceEvent]] = defaultdict(list)
    for e in sorted(events, key=lambda e: (e.event_time, e.event_id)):
        by_order[e.order_id].append(e)
    spans_days = 0
    for history in by_order.values():
        assert [e.version for e in history] == list(range(1, len(history) + 1))
        assert history[0].status == "created"
        for before, after in pairwise(history):
            assert after.status in {s for s, _ in NEXT_STATUS[before.status]}
        spans_days += len({e.event_date for e in history}) > 1
    assert spans_days > 0  # merges must reconcile orders across partitions


def test_events_land_in_their_own_day() -> None:
    for e in generate_events("2026-02-27", 3, 50, seed=4):
        assert e.event_time.startswith(e.event_date + " ")


@pytest.mark.parametrize(
    ("days", "per_day", "rate"),
    [
        (0, 10, 0.0),
        (3661, 10, 0.0),
        (1, 0, 0.0),
        (1, 100_001, 0.0),
        (1, 10, 0.6),
        (100, 100_000, 0.0),
    ],
)
def test_generator_bounds(days: int, per_day: int, rate: float) -> None:
    with pytest.raises(SpecError):
        generate_events("2026-01-01", days, per_day, seed=1, duplicate_rate=rate)


E1 = SourceEvent("e1", "o1", 1, "created", 500, "2026-01-01 10:00:00", "2026-01-01")
E2 = SourceEvent("e2", "o1", 2, "paid", 450, "2026-01-02 09:00:00", "2026-01-02")
E3 = SourceEvent("e3", "o2", 1, "created", 900, "2026-01-02 11:00:00", "2026-01-02")


def test_oracle_by_hand() -> None:
    events = [E2, E1, E3, E1]
    assert expected_rows("order_events", events) == {
        ("e1",): ("e1", "o1", 1, "created", 500, "2026-01-01 10:00:00", "2026-01-01"),
        ("e2",): ("e2", "o1", 2, "paid", 450, "2026-01-02 09:00:00", "2026-01-02"),
        ("e3",): ("e3", "o2", 1, "created", 900, "2026-01-02 11:00:00", "2026-01-02"),
    }
    assert expected_rows("orders", events) == {
        ("o1",): ("o1", 2, "paid", 450, "2026-01-02 09:00:00"),
        ("o2",): ("o2", 1, "created", 900, "2026-01-02 11:00:00"),
    }
    assert expected_rows("orders", events, {"2026-01-01"}) == {
        ("o1",): ("o1", 1, "created", 500, "2026-01-01 10:00:00"),
    }
    assert expected_rows("daily_order_stats", events) == {
        ("2026-01-01", "created"): ("2026-01-01", "created", 1, 500, "2026-01-01 10:00:00"),
        ("2026-01-02", "paid"): ("2026-01-02", "paid", 1, 450, "2026-01-02 09:00:00"),
        ("2026-01-02", "created"): ("2026-01-02", "created", 1, 900, "2026-01-02 11:00:00"),
    }
    with pytest.raises(SpecError):
        expected_rows("nope", events)


def test_compare_counts_each_kind_of_defect() -> None:
    expected = expected_rows("orders", [E1, E2, E3])
    actual = [
        ("o1", 2, "paid", 450, "2026-01-02 09:00:00"),
        ("o1", 2, "paid", 450, "2026-01-02 09:00:00"),  # duplicate
        ("o9", 1, "created", 1, "2026-01-02 11:00:00"),  # unexpected; o2 is missing
    ]
    assert compare("orders", actual, expected) == (1, 1, 1, 0)
    wrong = [
        ("o1", 1, "created", 500, "2026-01-01 10:00:00"),
        ("o2", 1, "created", 900, "2026-01-02 11:00:00"),
    ]
    assert compare("orders", wrong, expected) == (0, 0, 0, 1)
