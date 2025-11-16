from pulao.constant import TrendDirection


class Trend:
    direction: TrendDirection  # up / down / range
    start_index: int
    end_index: int
    high_price: float
    low_price: float
    strength: float

    def __init__(self, direction: TrendDirection, start_index: int, end_index: int):
        self.direction = direction
        self.start_index = start_index
        self.end_index = end_index

    @property
    def length(self):
        return self.end_index - self.start_index + 1

