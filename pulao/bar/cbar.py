from __future__ import annotations

from dataclasses import dataclass

from pulao.constant import SwingPointType, SwingPointLevel


@dataclass
class CBar:
    index : int # cbar_df index
    start_index: int = 0 # sbar_df index
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


class Fractal:
    left: CBar
    middle: CBar
    right: CBar

    def __init__(self, left: CBar, middle: CBar, right: CBar):
        self.left = left
        self.middle = middle
        self.right = right

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
