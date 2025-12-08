from __future__ import annotations

import math
from dataclasses import dataclass

from pulao.constant import Direction
from datetime import datetime as Datetime

@dataclass
class Swing:
    """
    一次推动力量，其间没有明显反抗力量，由分形一顶一底相连构成
    """
    id: int = None # swing_df 中 自增id
    direction: Direction = None
    cbar_start_id: int = None  # 波段起始点id cbar_df
    cbar_end_id: int = None
    sbar_start_id: int = None  # sbar_df id
    sbar_end_id: int = None
    high_price: float = 0
    low_price: float = 0

    span: float = 0 # 横跨多少根sbar

    is_completed: bool = False  # 波段是否完成
    created_at: Datetime = None  # 创建时间

    def __post_init__(self):
        if isinstance(self.direction, int):
            self.direction = Direction(self.direction)

    @property
    def distance(self):
        return self.high_price - self.low_price

    @property
    def slope(self):
        if self.span == 0:
            return 0
        return self.distance / self.span

    @property
    def angle(self):
        return math.degrees(math.atan(self.slope))

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    def overlap(self, *others: Swing):
        """
         判断当前 Swing 与任意多个 Swing 是否有重叠
        :param others: 一个或多个 Swing 对象
        :return: True 或 False
        """
        if not others:
            return False
        # 当前 Swing 的 low/high
        low_all = [self.low_price]
        high_all = [self.high_price]
        for other in others:
            if other is None:
                continue
            low_all.append(other.low_price)
            high_all.append(other.high_price)

        # 最大低点 <= 最小高点 表示有重叠
        return max(low_all) <= min(high_all)
