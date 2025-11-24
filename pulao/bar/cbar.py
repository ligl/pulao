from __future__ import annotations

from dataclasses import dataclass

from pulao.constant import SwingPointType, SwingPointLevel


@dataclass
class CBar:
    index: int  # cbar_df index
    start_index: int = 0  # sbar_df index
    end_index: int = 0
    high_price: float = 0
    low_price: float = 0

    swing_point_type: SwingPointType = SwingPointType.NONE
    swing_point_level: SwingPointLevel = SwingPointLevel.NONE
    swing_point_level_origin: SwingPointLevel = SwingPointLevel.NONE

    def __post_init__(self):
        if isinstance(self.swing_point_type, int):
            self.swing_point_type = SwingPointType(self.swing_point_type)
        if isinstance(self.swing_point_level, int):
            self.swing_point_level = SwingPointLevel(self.swing_point_level)
        if isinstance(self.swing_point_level_origin, int):
            self.swing_point_level_origin = SwingPointLevel(self.swing_point_level_origin)

    @property
    def length(self):
        return self.end_index - self.start_index + 1

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
