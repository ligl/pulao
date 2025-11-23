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


class Fractal:
    def __init__(self, left: CBar, middle: CBar, right: CBar):
        self.left: CBar = left
        self.middle: CBar = middle
        self.right: CBar = right

    def range(self):
        """
        计算分形的区间
        :return: low,high
        """
        low = min(self.left.low_price, self.middle.low_price, self.right.low_price)
        high = max(self.left.high_price, self.middle.high_price, self.right.high_price)
        return low, high

    def overlap(self, other: Fractal):
        """
        判断两个分形是否重叠
        :param other: Fractal
        :return: True or False
        """
        if other is None:
            return False
        low1, high1 = self.range()
        low2, high2 = other.range()
        return max(low1, low2) <= min(high1, high2)

    def validate(self) -> bool:
        return (
            Fractal.is_fractal(self.left, self.middle, self.right)
            != SwingPointType.NONE
        )

    @classmethod
    def is_fractal(cls, left: CBar, middle: CBar, right: CBar) -> SwingPointType:
        if left is None or middle is None or right is None:
            return SwingPointType.NONE

        if left.high_price < middle.high_price > right.high_price:  # 顶分形
            return SwingPointType.HIGH
        elif left.low_price > middle.low_price < right.low_price:  # 底分形
            return SwingPointType.LOW
        else:  # 不是分形
            return SwingPointType.NONE
