from pulao.constant import SwingPoint

from datetime import datetime as Datetime

from vnpy.trader.object import BarData


class SBar:
    """
    BarData扩展
    """

    symbol: str
    exchange: str
    interval: str

    datetime: Datetime
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0

    swing_point: SwingPoint = SwingPoint.NONE

    ema_20: float = 0
    ema_60: float = 0

    def __init__(self, bar: BarData):
        super().__init__()
        self.symbol = bar.symbol
        self.exchange = bar.exchange.value
        self.datetime = bar.datetime
        self.interval = bar.interval.value if bar.interval else ""
        self.volume = bar.volume
        self.turnover = bar.turnover
        self.open_interest = bar.open_interest
        self.open_price = bar.open_price
        self.high_price = bar.high_price
        self.low_price = bar.low_price
        self.close_price = bar.close_price

        self.swing_point = SwingPoint.NONE

        self.ema_20 = 0
        self.ema_60 = 0

    def __repr__(self):
        return f"SBar({self.__dict__})"

    def _repr_html_(self):
        self.__repr__()

    @property
    def body(self) -> float:
        """实体长度"""
        return abs(self.close_price - self.open_price)

    @property
    def upper_shadow(self) -> float:
        """上影线长度"""
        return self.high_price - max(self.close_price, self.open_price)

    @property
    def lower_shadow(self) -> float:
        """下影线长度"""
        return min(self.close_price, self.open_price) - self.low_price

    @property
    def total_range(self) -> float:
        """K线总波幅"""
        return self.high_price - self.low_price

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
        if self.close_price > self.open_price:
            return 1
        elif self.close_price < self.open_price:
            return -1
        return 0

    @property
    def is_swing_high(self) -> bool:
        return self.swing_point == SwingPoint.HIGH

    @property
    def is_swing_low(self) -> bool:
        return self.swing_point == SwingPoint.LOW

    def update_swing_point(self, sp: SwingPoint) -> None:
        self.swing_point = sp
