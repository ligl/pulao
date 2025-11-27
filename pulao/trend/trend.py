from dataclasses import dataclass

from pulao.constant import Direction


@dataclass
class Trend:
    id: int = None
    direction: Direction = None  # up / down / range
    start_id: int = 0 # swing_df
    end_id: int = 0
    high_price: float = 0
    low_price: float = 0
    strength: float = 0
    is_completed: bool = False  # 趋势是否完成

    @property
    def distance(self):
        return self.high_price - self.low_price

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    def price_ratio(self, price: float) -> float:
        span = max(self.high_price - self.low_price, 1e-9)
        if self.direction == Direction.UP:
            return (price - self.low_price) / span
        elif self.direction == Direction.DOWN:
            return (self.high_price - price) / span
        return (price - self.low_price) / span

