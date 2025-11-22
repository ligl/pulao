from typing import Optional, List

from .indicator_base import BaseIndicator
from pulao.bar import SBar


# -----------------------------
# EMA indicator
# -----------------------------

class EmaIndicator(BaseIndicator):
    def __init__(self, period: int):
        super().__init__(name=f"ema_{period}")
        if period <= 0:
            raise ValueError("period must be > 0")
        self.period = period
        self.alpha = 2.0 / (period + 1)
        self.value: Optional[float] = None

    def reset(self) -> None:
        self.value = None

    def update(self, bar: SBar) -> Optional[float]:
        price = float(bar.close_price)
        if self.value is None:
            # warm start: use first close as initial EMA
            self.value = price
        else:
            self.value = self.value + self.alpha * (price - self.value)
        return self.value

    def backfill(self, closes: List[float], start_index: int = 0) -> List[float]:
        # If starting at 0, we fully recompute; otherwise continue from self.value
        vals: List[float] = []
        if start_index == 0:
            self.value = None
        # set initial value from previous index if available
        for i in range(start_index, len(closes)):
            price = float(closes[i])
            if self.value is None:
                self.value = price
            else:
                self.value = self.value + self.alpha * (price - self.value)
            vals.append(self.value)
        return vals
