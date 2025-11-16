from __future__ import annotations

from .indicator_base import BaseIndicator
from typing import Any, Dict, List

import polars as pl

from .atr import AtrIndicator
from pulao.sbar import SBar

# -----------------------------
# IndicatorManager: manage multiple indicators per symbol
# -----------------------------

class IndicatorManager:
    """Manage a set of indicators for a single symbol/series.

    Features:
    - register indicators (EMA, ATR, custom)
    - update(bar): incremental update, O(1) per indicator
    - mark_dirty(idx): when historical bars modified, mark index to recompute
    - recompute_if_needed(df): efficiently recompute values from dirty index using Polars arrays
    - flush_to_polars(): get a Polars DataFrame with indicator columns aligned to current DF
    """

    def __init__(self):
        self.indicators: Dict[str, BaseIndicator] = {}
        # per-indicator storage of outputs
        self.outputs: Dict[str, List[Any]] = {}
        # the minimal index from which recompute is needed
        self.dirty_index: int = 0

    # registration
    def register(self, indicator: BaseIndicator) -> None:
        if indicator.name in self.indicators:
            raise ValueError(f"Indicator {indicator.name} already registered")
        self.indicators[indicator.name] = indicator
        self.outputs[indicator.name] = []

    def reset(self) -> None:
        for ind in self.indicators.values():
            ind.reset()
        for k in self.outputs.keys():
            self.outputs[k] = []
        self.dirty_index = 0

    # per-bar incremental update
    def update(self, bar: SBar) -> Dict[str, Any]:
        """Update all registered indicators with a new SBar value.
        Returns dict of name->value for this bar (useful for immediate write-back).
        """
        row: Dict[str, Any] = {}
        for name, ind in self.indicators.items():
            val = ind.update(bar)
            self.outputs[name].append(val)
            row[name] = val
        # new bar appended -> dirty_index moves forward
        self.dirty_index = max(self.dirty_index, len(next(iter(self.outputs.values()))))
        return row

    def mark_dirty(self, idx: int) -> None:
        """Indicate that bars at or after idx may have changed and indicators must be recomputed from idx."""
        self.dirty_index = min(self.dirty_index, idx) if self.dirty_index else idx

    def recompute_if_needed(self, df: pl.DataFrame) -> None:
        """If dirty_index < len(df), recompute outputs from dirty_index.

        df is expected to contain columns required by indicators: close_price, high_price, low_price, etc.
        """
        if self.dirty_index >= df.height():
            return

        start = self.dirty_index
        n = df.height()

        # Extract arrays once
        closes = df.select(pl.col("close_price")).to_series().to_list()
        highs = df.select(pl.col("high_price")).to_series().to_list()
        lows = df.select(pl.col("low_price")).to_series().to_list()

        # For each indicator, we backfill values from start
        for name, ind in self.indicators.items():
            if isinstance(ind, AtrIndicator):
                vals = ind.backfill(highs, lows, closes, start_index=start)
            else:
                vals = ind.backfill(closes, start_index=start)

            out = self.outputs[name]
            # ensure out has length == start
            out = out[:start]
            out.extend(vals)
            self.outputs[name] = out

        self.dirty_index = n

    def flush_to_polars(self) -> pl.DataFrame:
        """Return a Polars DataFrame with indicator columns aligned to stored outputs.
        Length equals existing outputs length (prefer to call after recompute_if_needed to align with base DF)
        """
        if not self.outputs:
            return pl.DataFrame()
        length = len(next(iter(self.outputs.values())))
        data = {name: (vals + [None] * (length - len(vals))) for name, vals in self.outputs.items()}
        return pl.DataFrame(data)


# -----------------------------
# Small helper: join indicator dataframe to main DF
# -----------------------------

def join_indicators_to_df(base_df: pl.DataFrame, ind_df: pl.DataFrame) -> pl.DataFrame:
    """Assumes both frames are aligned by row order. Adds indicator columns to base_df.

    This function handles length mismatch by truncation or padding.
    """
    if ind_df.height() == 0:
        return base_df

    # if lengths differ, make them equal by truncation/padding
    base_n = base_df.height()
    ind_n = ind_df.height()
    if ind_n < base_n:
        # pad ind_df with nulls
        pad = base_n - ind_n
        pad_df = pl.DataFrame({c: [None] * pad for c in ind_df.columns})
        ind_df = ind_df.vstack(pad_df)
    elif ind_n > base_n:
        ind_df = ind_df.head(base_n)

    # horizontally concat (zero-copy when possible)
    cols = base_df.columns + ind_df.columns
    values = [base_df[col] for col in base_df.columns] + [ind_df[col] for col in ind_df.columns]
    return pl.DataFrame({name: col for name, col in zip(cols, values)})
