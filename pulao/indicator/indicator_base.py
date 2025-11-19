
from __future__ import annotations

from typing import Any, List

from pulao.object import Base
from pulao.sbar import SBar


# -----------------------------
# Indicator base classes
# -----------------------------

class BaseIndicator(Base):

    name: str

    def __init__(self, name: str):
        self.name = name

    def reset(self) -> None:
        """Reset internal state."""
        raise NotImplementedError

    def update(self, bar: SBar) -> Any:
        """Incrementally compute indicator value for next bar.
        Must return the new indicator value (or None if not ready).
        """
        raise NotImplementedError

    def backfill(self, closes: List[float], start_index: int = 0) -> List[Any]:
        """Recompute indicator values from start_index over closes.
        Returns list of values aligning with closes[start_index:].
        Implementations can assume `self` is reset or has prior state consistent with start_index.
        """
        raise NotImplementedError
