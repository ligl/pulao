from typing import List

from pulao.constant import SwingDirection
from pulao.events import Observable
from pulao.sbar import SBar


class Swing:
    id: int
    direction: SwingDirection = SwingDirection.NONE  # "up" / "down"
    start_index: int  # SBar在SBarManager中的索引
    end_index: int
    high_price: float
    low_price: float
    strength: float

    def __init__(
        self,
        direction: SwingDirection,
        start_index: int = 0,
        end_index: int = 0,
        high_price: float = 0,
        low_price: float = 0,
    ):
        super().__init__()
        self.direction = direction  # "up" or "down"
        self.start_index = start_index
        self.end_index = end_index
        self.high_price = high_price
        self.low_price = low_price

    @property
    def length(self):
        return self.end_index - self.start_index + 1

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    def price_ratio(self, price: float) -> float:
        span = max(self.high_price - self.low_price, 1e-9)
        if self.direction == SwingDirection.UP:
            return (price - self.low_price) / span
        elif self.direction == SwingDirection.DOWN:
            return (self.high_price - price) / span
        return (price - self.low_price) / span


class SwingManager(Observable):
    swings: List[Swing]

    def __init__(self):
        super().__init__()
        self.swings = []

    def add(self, swing: Swing):
        self.swings.append(swing)
        self.notify("swing.created", swing)
