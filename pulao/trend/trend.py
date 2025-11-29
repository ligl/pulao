from dataclasses import dataclass

from pulao.constant import Direction
from datetime import datetime as Datetime

@dataclass
class Trend:
    """
    一个方向上力量占优的波段序列，每个趋势至少包含3个有重叠的波段（允许更长波段序列）
    """
    id: int = None
    direction: Direction = None  # up / down / range
    swing_start_id: int = None # swing_df
    swing_end_id: int = None
    high_price: float = 0
    low_price: float = 0

    sbar_start_id: int = None # sbar_df id
    sbar_end_id: int = None

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

    @property
    def opposite_direction(self):
        """
        获取波段对立的方向
        """
        if self.direction == Direction.UP:
            return Direction.DOWN
        else:
            return Direction.UP

