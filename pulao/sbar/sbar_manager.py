from typing import Any

from polars import Datetime

from pulao.events import Observable
from pulao.indicator import IndicatorManager, EmaIndicator
from .sbar import SBar
import polars as pl

from ..constant import EventType


class SBarManager(Observable):
    """
    管理缓存bar数据，计算指标
    """

    df: pl.DataFrame = None
    indicator_manager: IndicatorManager

    def __init__(self):
        super().__init__()
        schema = {
            "datetime": pl.Datetime,
            "symbol": pl.Utf8,
            "exchange": pl.Utf8,
            "interval": pl.Utf8,
            "open_price": pl.Float32,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "close_price": pl.Float32,
            "volume": pl.Float32,  # 部分品种成交量是浮点
            "open_interest": pl.Float32,
            "swing_point_type": pl.Utf8,  # 波段高低点标记
            "swing_point_level": pl.Int32,  # 波段高低点级别
            "ema_20": pl.Float32,
            "ema_60": pl.Float32,
        }
        self.df = pl.DataFrame(schema=schema)

        self.indicator_manager = IndicatorManager()
        self.indicator_manager.register(EmaIndicator(20))
        self.indicator_manager.register(EmaIndicator(60))

    def append(self, sbar: SBar) -> int:
        sbar.index = self.df.height
        # 计算ema20、ema60指标
        indicator_dict = self.indicator_manager.update(sbar)
        sbar.ema_20 = indicator_dict["ema_20"]
        sbar.ema_60 = indicator_dict["ema_60"]

        row = sbar.to_schema()

        self.df = self.df.vstack(
            pl.DataFrame(
                [[row[col] for col in self.df.columns]],
                schema=self.df.schema,
                orient="row",
            )
        )  # append row
        self.notify(EventType.SBAR_CREATED, sbar)
        return sbar.index

    def get_at_index(self, index: int) -> SBar:
        row = self.df.row(index, named=True)
        sbar = SBar()
        sbar.index = index
        sbar.exchange = row["exchange"]
        sbar.symbol = row["symbol"]
        sbar.interval = row["interval"]
        sbar.open_price = row["open_price"]
        sbar.high_price = row["high_price"]
        sbar.low_price = row["low_price"]
        sbar.close_price = row["close_price"]
        sbar.volume = row["volume"]
        sbar.open_interest = row["open_interest"]
        sbar.swing_point_type = row["swing_point_type"]
        sbar.swing_point_level = row["swing_point_level"]
        sbar.ema_20 = row["ema_20"]
        sbar.ema_60 = row["ema_60"]

        return sbar

    def get_at_time(self, dt: Datetime) -> SBar:
        index = self.df.select(pl.col("datetime").search_sorted(dt)).item()
        return self.get_at_index(index)

    def get_range_index(self, start: int, end: int):
        return self.df.slice(start, end - start + 1)

    def get_range_time(self, start: Datetime, end: Datetime):
        start_idx = self.df.select(pl.col("datetime").search_sorted(start)).item()
        end_idx = self.df.select(
            pl.col("datetime").search_sorted(end, side="right")
        ).item()

        return self.df.slice(start_idx, end_idx - start_idx)

    @property
    def total_count(self):
        return self.df.height

    def get_last(self, length: int = 1):
        return self.df.slice(-length)

    def update_by_datetime(self, dt, field, value):
        self.df = self.df.with_columns(
            pl.when(pl.col("datetime") == dt)
            .then(pl.lit(value))
            .otherwise(pl.col(field))
            .alias(field)
        )

    def update_by_index(self, index, field, value):
        self.df = self.df.with_columns(
            pl.when(pl.arange(0, self.df.height) == index)
            .then(pl.lit(value))
            .otherwise(pl.col(field))
            .alias(field)
        )
def _sbar_to_row(bar: SBar) -> dict:
    return {
        "symbol": bar.symbol,
        "exchange": bar.exchange,
        "interval": bar.interval,
        "datetime": bar.datetime,
        "volume": bar.volume,
        "open_interest": bar.open_interest,
        "open_price": bar.open_price,
        "high_price": bar.high_price,
        "low_price": bar.low_price,
        "close_price": bar.close_price,
        "swing_point_type": bar.swing_point_type.value,
        "swing_point_level": bar.swing_point_type.value,
        "ema_20": bar.ema_20,
        "ema_60": bar.ema_60,
    }


