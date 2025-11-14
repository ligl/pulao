"""
Basic data structure for pulao
"""  # noqa: SPELLING
from typing import Dict

from vnpy.trader.object import BarData


class PulaoBar:  # noqa: SPELLING
    """
    vnpy BarData扩展
    """

    bar: BarData

    def __init__(self, bar: BarData):
        super().__init__()
        self.bar = bar

    @property
    def body(self) -> float:
        """实体长度"""
        return abs(self.bar.close_price - self.bar.open_price)

    @property
    def upper_shadow(self) -> float:
        """上影线长度"""
        return self.bar.high_price - max(self.bar.close_price, self.bar.open_price)

    @property
    def lower_shadow(self) -> float:
        """下影线长度"""
        return min(self.bar.close_price, self.bar.open_price) - self.bar.low_price

    @property
    def total_range(self) -> float:
        """K线总波幅"""
        return self.bar.high_price - self.bar.low_price

    @property
    def body_ratio(self) -> float:
        """实体占总波幅比例"""
        return self.body / self.total_range if self.total_range else 0

    @property
    def shadow_ratio(self) -> float:
        """影线总长度占波幅比例"""
        if self.total_range == 0:
            return 0
        return (self.upper_shadow + self.lower_shadow) / self.total_range

    @property
    def direction(self) -> int:
        """
        方向：
        +1 = 阳线，-1 = 阴线，0 = 平盘
        """
        if self.bar.close_price > self.bar.open_price:
            return 1
        elif self.bar.close_price < self.bar.open_price:
            return -1
        return 0


class Structure:
    """
    识别走势结构、方向与强度
    """

    def update(self, bar:PulaoBar) -> Dict[str, float]:
        ...



class KeyZone:
    """
    提取关键区间、兴趣区域
    """

    ...


class Signal:
    """
    在关键区识别供需变化信号
    """

    ...


class Decision:
    """
    综合趋势、位置、信号，生成交易建议
    """

    ...
