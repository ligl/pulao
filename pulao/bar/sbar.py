from dataclasses import dataclass

from datetime import datetime as Datetime

@dataclass
class SBar:
    """
    SuperBar , BarData扩展
    """

    index: int # 在SBarManager数据源中的索引

    symbol: str = None
    exchange: str = None
    interval: str = None

    datetime: Datetime = None
    volume: float = 0
    turnover: float = 0
    open_interest: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    close_price: float = 0

    ema_short: float = 0
    ema_long: float = 0

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
