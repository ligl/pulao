from pulao.constant import TrendDirection
from pulao.object import BaseDecorator


@BaseDecorator()
class Trend:
    direction: TrendDirection = TrendDirection.NONE  # up / down / range
    start_index: int = 0 # cbar_df
    end_index: int = 0
    high_price: float = 0
    low_price: float = 0
    strength: float = 0
    start_index_bar: int = 0  # 趋势中bar对应数据源的索引 sbar_df
    end_index_bar: int = 0
    is_completed: bool = False  # 趋势是否完成

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
        if self.direction == TrendDirection.UP:
            return (price - self.low_price) / span
        elif self.direction == TrendDirection.DOWN:
            return (self.high_price - price) / span
        return (price - self.low_price) / span

    def get_swings(self):
        """
        获取组成趋势的波段列表
        """
