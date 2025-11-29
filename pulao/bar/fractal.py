from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple,Generic, TypeVar

from pulao.bar import CBar
from pulao.constant import FractalType
from pulao.swing import Swing

T = TypeVar('T', CBar, Swing)

@dataclass
class Fractal:
    left: T
    middle: T
    right: T

    def range(self) -> Tuple[float, float]:
        """
        计算分形的区间
        :return: low,high
        """
        low = min(self.left.low_price, self.middle.low_price, self.right.low_price)
        high = max(self.left.high_price, self.middle.high_price, self.right.high_price)
        return low, high

    @property
    def id(self) -> int:
        """
        middle.index
        """
        return self.middle.id

    @property
    def cbar_start_id(self) ->int:
        """
        left.index
        """
        return self.left.id

    @property
    def cbar_end_id(self) ->int:
        """
        right.index
        """
        return self.right.id

    @property
    def sbar_start_id(self) -> int:
        """
        left.start_index
        """
        return self.left.sbar_start_id

    @property
    def sbar_end_id(self) -> int:
        """
        right.end_index
        """
        return self.right.sbar_end_id

    def sbar_middle_id(self):
        return self.middle.sbar_start_id

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

    def fractal_type(self, strict_model:bool = True) -> FractalType:
        return Fractal.verify(self.left, self.middle, self.right, strict_model=strict_model)

    @classmethod
    def verify(cls, left: T, middle: T, right: T, strict_model:bool = False) -> FractalType:
        """
        判断分形
        :param left: left cbar
        :param middle:  middle cbar
        :param right: right cbar
        :param strict_model: 是否严格验证，False:只对比价格关系，True：不仅对比价格关系，还检验middle cbar的类型是否正常
        :return:
        """
        if left is None or middle is None or right is None:
            return FractalType.NONE

        if left.high_price < middle.high_price > right.high_price:  # 顶分形
            if strict_model:
                return FractalType.TOP if middle.fractal_type == FractalType.TOP else FractalType.NONE
            return FractalType.TOP
        elif left.low_price > middle.low_price < right.low_price:  # 底分形
            if strict_model:
                return FractalType.BOTTOM if middle.fractal_type == FractalType.BOTTOM else FractalType.NONE
            return FractalType.BOTTOM

        return FractalType.NONE
