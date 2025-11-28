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
    start_id: int = None  # 波段起始点id cbar_df
    end_id: int = None
    high_price: float = 0
    low_price: float = 0
    sbar_start_id: int = None # sbar_df id
    sbar_end_id: int = None

    is_completed: bool = False  # 波段是否完成
    created_at: Datetime = None  # 创建时间

    def __post_init__(self):
        if isinstance(self.direction, int):
            self.direction = Direction(self.direction)

    @property
    def distance(self):
        return self.high_price - self.low_price

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    @property
    def opposite_direction(self):
        """
        获取波段对立的方向
        """
        if self.direction == Direction.UP:
            return Direction.DOWN
        else:
            return Direction.UP

