from __future__ import annotations

from enum import Enum
import math
from dataclasses import dataclass

from pulao.constant import Direction
from datetime import datetime as Datetime

from pulao.sd import SupplyDemand

class SwingState(Enum):
    """
    波段状态枚举
    Extending = 1  # 延续状态，当前方向正在延展
    Tentative = 2  # 候选状态，已出现反向候选，等待后继 swing 证明其有效
    Confirmed = 3  # 确认状态，已被后继反向 swing 确认终结
    """
    Extending = 1  # 延续状态，当前方向正在延展
    Tentative = 2  # 候选状态，已出现反向候选，等待后继 swing 证明其有效
    Confirmed = 3  # 确认状态，已被后继反向 swing 确认终结

@dataclass(slots=True)
class Swing:
    """
    一次推动力量，其间没有明显反抗力量，由分形一顶一底相连构成
    """
    id: int # swing_df 中 自增id
    direction: Direction
    cbar_start_id: int  # 波段起始点id cbar_df
    cbar_end_id: int
    sbar_start_id: int  # sbar_df id
    sbar_end_id: int
    high_price: float
    low_price: float

    start_oi: float
    end_oi: float
    volume: float
    span: float = 0 # 横跨多少根sbar

    state : SwingState = SwingState.Extending  # 波段状态
    created_at: Datetime = Datetime.now()  # 创建时间

    def __post_init__(self):
        if isinstance(self.direction, int):
            self.direction = Direction(self.direction)
        if isinstance(self.state, int):
            self.state = SwingState(self.state)

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

    @property
    def is_completed(self) -> bool:
        return self.state == SwingState.Confirmed

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

    def sd(self) -> SupplyDemand:
        raise NotImplementedError
