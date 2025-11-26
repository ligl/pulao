from __future__ import annotations

from dataclasses import dataclass

from ..constant import FractalType
from datetime import datetime as Datetime

@dataclass
class CBar:
    id: int = None  # cbar_df primary key 类似数据库中的自增id
    start_id: int = None  # sbar_df id
    end_id: int = None
    high_price: float = 0
    low_price: float = 0
    created_at: Datetime = None # 创建时间

    fractal_type: FractalType = FractalType.NONE

    def __post_init__(self):
        if isinstance(self.fractal_type, int):
            self.fractal_type = FractalType(self.fractal_type)

    @property
    def distance(self):
        return self.high_price - self.low_price

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    def is_inclusive(self, other: CBar) -> bool:
        if self.low_price <= other.low_price and self.high_price >= other.high_price:
            return True  # 内包
        if self.low_price >= other.low_price and self.high_price <= other.high_price:
            return True  # 外包
        return False
