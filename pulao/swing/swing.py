from dataclasses import dataclass

from pulao.constant import SwingDirection


@dataclass
class Swing:
    """
    一次推动力量，其间没有明显反抗力量，由分形一顶一底相连构成
    """
    index: int
    direction: SwingDirection
    start_index: int = 0  # 波段起始点索引 cbar_df
    end_index: int = 0
    high_price: float = 0
    low_price: float = 0

    is_completed: bool = False  # 波段是否完成

    def __post_init__(self):
        if isinstance(self.direction, int):
            self.direction = SwingDirection(self.direction)

    @property
    def length(self):
        return self.end_index - self.start_index + 1

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
        if self.direction == SwingDirection.UP:
            return SwingDirection.DOWN
        else:
            return SwingDirection.UP

