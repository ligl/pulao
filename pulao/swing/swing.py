from dataclasses import dataclass

from pulao.constant import SwingDirection


@dataclass
class Swing:
    index: int = 0
    direction: SwingDirection = SwingDirection.NONE  # "up" / "down"
    start_index: int = 0  # 波段起始点索引 cbar_df
    end_index: int = 0
    high_price: float = 0
    low_price: float = 0
    strength: float = 0
    start_index_bar: int = 0  # 波段中bar对应数据源的索引 sbar_df
    end_index_bar: int = 0

    is_completed: bool = False  # 波段是否完成

    @property
    def length(self):
        return self.end_index - self.start_index + 1

    @property
    def distance(self):
        return self.high_price - self.low_price

    def contains(self, price: float) -> bool:
        return self.low_price <= price <= self.high_price

    def price_ratio(self, price: float) -> float:
        span = max(self.high_price - self.low_price, 1e-9)
        if self.direction == SwingDirection.UP:
            return (price - self.low_price) / span
        elif self.direction == SwingDirection.DOWN:
            return (self.high_price - price) / span
        return (price - self.low_price) / span

