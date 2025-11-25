from typing import List

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

    def __init__(self):
        super().__init__()
        schema = {
            "index": pl.UInt32,
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
            "ema_short": pl.Float32,
            "ema_long": pl.Float32,
        }
        self.df_sbar: pl.DataFrame = pl.DataFrame(schema=schema)

        self.indicator_manager: IndicatorManager = IndicatorManager()
        self.indicator_manager.register(EmaIndicator(20))
        self.indicator_manager.register(EmaIndicator(60))

    def append(self, sbar: SBar) -> int:
        # 为sbar设置index，唯一的
        sbar.index = self.df_sbar.height
        # 计算ema20、ema60指标
        indicator_dict = self.indicator_manager.update(sbar)
        sbar.ema_short = indicator_dict["ema_20"]
        sbar.ema_long = indicator_dict["ema_60"]

        row = {
            "index": sbar.index,
            "symbol": sbar.symbol,
            "exchange": sbar.exchange,
            "interval": sbar.interval,
            "datetime": sbar.datetime,
            "volume": sbar.volume,
            "open_interest": sbar.open_interest,
            "open_price": sbar.open_price,
            "high_price": sbar.high_price,
            "low_price": sbar.low_price,
            "close_price": sbar.close_price,
            "ema_short": sbar.ema_short,
            "ema_long": sbar.ema_long,
        }
        self.df_sbar = self.df_sbar.vstack(
            pl.DataFrame(
                [[row[col] for col in self.df_sbar.columns]],
                schema=self.df_sbar.schema,
                orient="row",
            )
        )  # append row
        self.notify(EventType.SBAR_CREATED, sbar)
        return sbar.index

    def get_at_index(self, index: int) -> SBar:
        return SBar(**self.df_sbar.row(index, named=True))

    def get_at_time(self, dt: Datetime) -> SBar:
        index = self.df_sbar.select(pl.col("datetime").search_sorted(dt)).item()
        return self.get_at_index(index)

    def get_sbar_list(self, start: int, end: int) -> List[SBar] | None:
        df = self.df_sbar.slice(start, end - start + 1)
        if df.is_empty():
            return None
        return [SBar(**row) for row in df.rows(named=True)]

    @property
    def total_count(self):
        return self.df_sbar.height

    def get_last(self, length: int = 1):
        return self.df_sbar.slice(-length)

    def update_by_datetime(self, dt: Datetime, field: str, value):
        self.df_sbar = self.df_sbar.with_columns(
            [
                pl.when(pl.col("datetime") == dt)
                .then(pl.lit(value))
                .otherwise(pl.col(field))
                .alias(field)
            ]
        )

    def update_by_index(self, index: int, field: str, value):
        self.df_sbar = self.df_sbar.with_columns(
            [
                pl.when(pl.col("index") == index)
                .then(pl.lit(value))
                .otherwise(pl.col(field))
                .alias(field)
            ]
        )

    def update(self, with_columns):
        self.df_sbar = self.df_sbar.with_columns(with_columns)

    def get_dataframe(self):
        return self.df_sbar
