from typing import Optional, List

from .indicator_base import BaseIndicator
from pulao.bar import SBar


# -----------------------------
# ATR indicator (Wilder style EMA of TR)
# -----------------------------


class AtrIndicator(BaseIndicator):
    def __init__(self, period: int):
        super().__init__(name=f"atr_{period}")
        if period <= 0:
            raise ValueError("period must be > 0")
        self.period = period
        self.value: Optional[float] = None
        self.prev_close: Optional[float] = None

    def reset(self) -> None:
        self.value = None
        self.prev_close = None

    def _true_range(
        self, high: float, low: float, prev_close: Optional[float]
    ) -> float:
        if prev_close is None:
            return high - low
        return max(high - low, abs(high - prev_close), abs(low - prev_close))

    def update(self, bar: SBar) -> Optional[float]:
        high = float(bar.high_price)
        low = float(bar.low_price)
        close = float(bar.close_price)
        tr = self._true_range(high, low, self.prev_close)
        self.prev_close = close
        if self.value is None:
            self.value = tr
        else:
            # Wilder smoothing: ATR = (prev_atr * (n-1) + tr) / n
            self.value = (self.value * (self.period - 1) + tr) / self.period
        return self.value

    def backfill(
        self,
        highs: List[float],
        lows: List[float],
        closes: List[float],
        start_index: int = 0,
    ) -> List[float]:
        # Backfill requires sequences of highs, lows, closes
        vals: List[float] = []
        if start_index == 0:
            self.value = None
            self.prev_close = None
        else:
            # assume prev_close already set externally if starting > 0
            pass
        for i in range(start_index, len(closes)):
            tr = self._true_range(float(highs[i]), float(lows[i]), self.prev_close)
            self.prev_close = float(closes[i])
            if self.value is None:
                self.value = tr
            else:
                self.value = (self.value * (self.period - 1) + tr) / self.period
            vals.append(self.value)
        return vals
