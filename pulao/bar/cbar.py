from pulao.constant import SwingPointType, SwingPointLevel
from pulao.object import BaseDecorator


@BaseDecorator()
class CBar:
    index : int = 0 # cbar_df index
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
