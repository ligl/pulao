from dataclasses import dataclass

from pulao.constant import Direction
from datetime import datetime as Datetime

@dataclass
class Trend:
    """
    由波段高低点关系组成的趋势结构
    """
    id: int = None
    direction: Direction = None  # up / down / range
    start_id: int = 0 # swing_df
    end_id: int = 0
    high_price: float = 0
    low_price: float = 0
    is_completed: bool = False  # 趋势是否完成
    created_at: Datetime = None  # 创建时间

    def __post_init__(self):
        if isinstance(self.direction, int):
            self.direction = Direction(self.direction)

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

