from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from pulao.bar import CBar
from pulao.constant import FractalType, SwingPointType


@dataclass
class Fractal:
    left: CBar
    middle: CBar
    right: CBar

    def range(self) -> Tuple[float, float]:
        """
        计算分形的区间
        :return: low,high
        """
        low = min(self.left.low_price, self.middle.low_price, self.right.low_price)
        high = max(self.left.high_price, self.middle.high_price, self.right.high_price)
        return low, high

    @property
    def index(self) -> int:
        """
        middle.index
        """
        return self.middle.index

    @property
    def start_index(self) ->int:
        """
        left.index
        """
        return self.left.index

    @property
    def end_index(self) ->int:
        """
        right.index
        """
        return self.right.index

    @property
    def high_price(self) -> float:
        _, high = self.range()
        return high

    @property
    def low_price(self) -> float:
        low, _ = self.range()
        return low

    def overlap(self, other: Fractal, is_strict=True):
        """
        判断两个分形是否重叠
        :param other: Fractal
        :param is_strict: 是否严格比较两个分形，True:比较两个分形的完整区间，False:只比较分形高低点所在k有没有重叠
        :return: True or False
        """
        if other is None:
            return False
        if is_strict: # 比较整个分形区间
            low1, high1 = self.range()
            low2, high2 = other.range()
        else: # 只比较分形高低点所在K
            low1, high1 = self.middle.low_price, self.middle.high_price
            low2, high2 = other.middle.low_price, self.middle.high_price
        return max(low1, low2) <= min(high1, high2)

    def valid(self) -> FractalType:
        return Fractal.is_fractal(self.left, self.middle, self.right)

    @classmethod
    def is_fractal(cls, left: CBar, middle: CBar, right: CBar) -> FractalType:
        if left is None or middle is None or right is None:
            return FractalType.NONE
        if left.high_price < middle.high_price > right.high_price:  # 顶分形
            return FractalType.TOP
        elif left.low_price > middle.low_price < right.low_price:  # 底分形
            return FractalType.BOTTOM
        else:  # 不是分形
            return FractalType.NONE
