from pulao.constant import SwingDirection, SwingPointType


class Swing:
    index: int
    direction: SwingDirection = SwingDirection.NONE  # "up" / "down"
    start_index: int # 波段起始点索引
    end_index: int
    high_price: float
    low_price: float
    strength: float
    start_index_bar : int # 波段中bar对应数据源的索引
    end_index_bar : int

    is_completed: bool # 波段是否完成

    def __init__(
        self,
        direction: SwingDirection = SwingDirection.NONE,
        start_index: int = 0,
        end_index: int = 0,
        high_price: float = 0,
        low_price: float = 0,
        is_completed: bool = False,
        start_index_bar: int = 0,
        end_index_bar: int = 0,
    ):
        super().__init__()
        self.direction = direction  # "up" or "down"
        self.start_index = start_index
        self.end_index = end_index
        self.high_price = high_price
        self.low_price = low_price
        self.is_completed = is_completed
        self.start_index_bar = start_index_bar
        self.end_index_bar = end_index_bar

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
        if self.direction == SwingDirection.UP:
            return (price - self.low_price) / span
        elif self.direction == SwingDirection.DOWN:
            return (self.high_price - price) / span
        return (price - self.low_price) / span

    def __repr__(self):
        return f"Swing({self.__dict__})"

    def _repr_html_(self):
        self.__repr__()

    def to_dict(self, include_private=False):
        if include_private:
            return self.__dict__
        else:
            return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
