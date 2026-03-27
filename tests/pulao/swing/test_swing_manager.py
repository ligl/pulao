from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl
import pytest

from pulao.bar.cbar_manager import CBarManager
from pulao.constant import Direction, EventType, Timeframe
from pulao.swing.swing import SwingState
from pulao.swing.swing_manager import SwingManager


@dataclass(slots=True)
class DummyFractal:
    id: int


class DummyCBarManager:
    def __init__(self):
        self.symbol = "TEST"
        self.timeframe = Timeframe.M5
        self._subs = []
        self.fractals: dict[int, DummyFractal] = {}

    def subscribe(self, fn, event_type):
        self._subs.append((fn, event_type))

    def get_fractal(self, cbar_id: int):
        return self.fractals.get(cbar_id)


@pytest.fixture
def manager() -> SwingManager:
    return SwingManager(DummyCBarManager())


@pytest.fixture
def populated_manager(manager: SwingManager) -> SwingManager:
    rows = [
        {
            "id": 10,
            "cbar_start_id": 100,
            "cbar_end_id": 109,
            "sbar_start_id": 1000,
            "sbar_end_id": 1009,
            "high_price": 110.0,
            "low_price": 100.0,
            "direction": Direction.UP.value,
            "span": 10,
            "volume": 1000.0,
            "start_oi": 10.0,
            "end_oi": 11.0,
            "state": SwingState.Confirmed.value,
            "created_at": datetime(2026, 1, 1, 0, 0, 0),
        },
        {
            "id": 20,
            "cbar_start_id": 110,
            "cbar_end_id": 119,
            "sbar_start_id": 1010,
            "sbar_end_id": 1019,
            "high_price": 109.0,
            "low_price": 90.0,
            "direction": Direction.DOWN.value,
            "span": 10,
            "volume": 1200.0,
            "start_oi": 11.0,
            "end_oi": 12.0,
            "state": SwingState.Confirmed.value,
            "created_at": datetime(2026, 1, 1, 0, 1, 0),
        },
        {
            "id": 30,
            "cbar_start_id": 120,
            "cbar_end_id": 129,
            "sbar_start_id": 1020,
            "sbar_end_id": 1029,
            "high_price": 116.0,
            "low_price": 95.0,
            "direction": Direction.UP.value,
            "span": 10,
            "volume": 1300.0,
            "start_oi": 12.0,
            "end_oi": 13.0,
            "state": SwingState.Tentative.value,
            "created_at": datetime(2026, 1, 1, 0, 2, 0),
        },
        {
            "id": 40,
            "cbar_start_id": 130,
            "cbar_end_id": 139,
            "sbar_start_id": 1030,
            "sbar_end_id": 1039,
            "high_price": 115.0,
            "low_price": 92.0,
            "direction": Direction.DOWN.value,
            "span": 10,
            "volume": 1400.0,
            "start_oi": 13.0,
            "end_oi": 14.0,
            "state": SwingState.Extending.value,
            "created_at": datetime(2026, 1, 1, 0, 3, 0),
        },
        {
            "id": 50,
            "cbar_start_id": 140,
            "cbar_end_id": 149,
            "sbar_start_id": 1040,
            "sbar_end_id": 1049,
            "high_price": 125.0,
            "low_price": 101.0,
            "direction": Direction.UP.value,
            "span": 10,
            "volume": 1500.0,
            "start_oi": 14.0,
            "end_oi": 15.0,
            "state": SwingState.Confirmed.value,
            "created_at": datetime(2026, 1, 1, 0, 4, 0),
        },
    ]
    manager.df_swing = pl.DataFrame(rows, schema=manager.df_swing.schema)
    return manager


def test_get_idx_and_get_swing_by_id(populated_manager: SwingManager):
    assert populated_manager.get_idx(30) == 2
    assert populated_manager.get_idx(999) is None

    swing = populated_manager.get_swing(30)
    assert swing is not None
    assert swing.id == 30
    assert swing.direction == Direction.UP


def test_get_last_swing(populated_manager: SwingManager):
    last = populated_manager.get_last_swing()
    assert last is not None
    assert last.id == 50


def test_get_nearest_swing(populated_manager: SwingManager):
    forward = populated_manager.get_nearest_swing(20, 2)
    assert forward is not None
    assert [s.id for s in forward] == [30, 40]

    backward = populated_manager.get_nearest_swing(40, -2)
    assert backward is not None
    assert [s.id for s in backward] == [20, 30]

    next_one = populated_manager.get_nearest_swing(20, 1)
    assert next_one is not None
    assert next_one.id == 30

    prev_one = populated_manager.get_nearest_swing(20, -1)
    assert prev_one is not None
    assert prev_one.id == 10


def test_prev_next_helpers(populated_manager: SwingManager):
    prev_opposite = populated_manager.prev_opposite_swing(30)
    assert prev_opposite is not None
    assert prev_opposite.id == 20

    next_opposite = populated_manager.next_opposite_swing(30)
    assert next_opposite is not None
    assert next_opposite.id == 40

    # first same-direction predecessor would be index 0, which get_swing_by_index guards out
    assert populated_manager.prev_same_swing(30) is None
    # same-direction successor would be last index, which get_swing_by_index guards out
    assert populated_manager.next_same_swing(30) is None


def test_get_limit_swing_and_limit_id(populated_manager: SwingManager):
    max_up = populated_manager.get_limit_swing(50, 10, "max", Direction.UP)
    assert max_up is not None
    assert max_up.id == 50

    min_down_id = populated_manager.get_limit_swing_id(10, 50, "min", Direction.DOWN)
    assert min_down_id == 20

    assert populated_manager.get_limit_swing(10, 50, "avg", Direction.UP) is None


def test_get_swing_fractal(populated_manager: SwingManager):
    cbar_manager = populated_manager.cbar_manager
    cbar_manager.fractals[110] = DummyFractal(id=110)
    cbar_manager.fractals[119] = DummyFractal(id=119)

    start_fractal, end_fractal = populated_manager.get_swing_fractal(20)
    assert start_fractal is not None
    assert end_fractal is not None
    assert start_fractal.id == 110
    assert end_fractal.id == 119

    none_start, none_end = populated_manager.get_swing_fractal(999)
    assert none_start is None
    assert none_end is None


def test_get_swing_list_and_completed_flag(populated_manager: SwingManager):
    swing_list = populated_manager.get_swing_list(20, 40)
    assert swing_list is not None
    assert [s.id for s in swing_list] == [20, 30, 40]

    completed = populated_manager.get_swing(50, is_completed=True)
    assert completed is not None
    assert completed.id == 50


def test_on_cbar_changed_without_backtrack_calls_build(monkeypatch, manager: SwingManager):
    called = {"build": 0, "clean": 0, "replay": 0, "notify": 0}

    monkeypatch.setattr(manager.swing_builder, "_build_swing", lambda: called.__setitem__("build", called["build"] + 1))
    monkeypatch.setattr(manager.swing_builder, "_clean_backtrack", lambda _id: called.__setitem__("clean", called["clean"] + 1))
    monkeypatch.setattr(manager.swing_builder, "_backtrack_replay", lambda _id: called.__setitem__("replay", called["replay"] + 1))
    monkeypatch.setattr(manager, "write_parquet", lambda: None)

    manager.subscribe(
        lambda timeframe, event, payload: called.__setitem__("notify", called["notify"] + 1),
        EventType.SWING_CHANGED,
    )

    manager._on_cbar_changed(Timeframe.M5, EventType.CBAR_CHANGED, payload={})

    assert called == {"build": 1, "clean": 0, "replay": 0, "notify": 1}


def test_on_cbar_changed_with_backtrack_calls_rebuild(monkeypatch, manager: SwingManager):
    called = {"build": 0, "clean": 0, "replay": 0}

    monkeypatch.setattr(manager.swing_builder, "_build_swing", lambda: called.__setitem__("build", called["build"] + 1))
    monkeypatch.setattr(manager.swing_builder, "_clean_backtrack", lambda _id: called.__setitem__("clean", called["clean"] + 1))
    monkeypatch.setattr(manager.swing_builder, "_backtrack_replay", lambda _id: called.__setitem__("replay", called["replay"] + 1))
    monkeypatch.setattr(manager, "write_parquet", lambda: None)

    manager._on_cbar_changed(Timeframe.M5, EventType.CBAR_CHANGED, payload={"backtrack_id": 12345})

    assert called == {"build": 0, "clean": 1, "replay": 1}
