from typing import List

from polars import Datetime

from pulao.events import Observable
from pulao.indicator import IndicatorManager, EmaIndicator
from .sbar import SBar
import polars as pl

from ..constant import EventType
from ..utils import IDGenerator
from datetime import datetime as Datetime


class SBarManager(Observable):
    """
    管理缓存bar数据，计算指标
    """

    def __init__(self):
        super().__init__()
        schema = {
            "id": pl.UInt64,
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
            "created_at": pl.Datetime("ms"),
        }
        self.df_sbar: pl.DataFrame = pl.DataFrame(schema=schema)

        self.indicator_manager: IndicatorManager = IndicatorManager()
        self.indicator_manager.register(EmaIndicator(20))
        self.indicator_manager.register(EmaIndicator(60))
        self.id_gen = IDGenerator()

    def append(self, sbar: SBar) -> int:
        # 为sbar设置index，唯一的
        sbar.id = self.id_gen.get_id()
        # 计算ema20、ema60指标
        indicator_dict = self.indicator_manager.update(sbar)
        sbar.ema_short = indicator_dict["ema_20"]
        sbar.ema_long = indicator_dict["ema_60"]

        row = {
            "id": sbar.id,
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
            "created_at": Datetime.now(),
        }
        self.df_sbar = self.df_sbar.vstack(
            pl.DataFrame(
                [[row[col] for col in self.df_sbar.columns]],
                schema=self.df_sbar.schema,
                orient="row",
            )
        )  # append row
        self.notify(EventType.SBAR_CREATED, sbar)
        return sbar.id

    def get_index(self, id: int) -> int:
        return self.df_sbar.select(pl.col("id").search_sorted(id)).item()

    def get_at_id(self, id: int) -> SBar:
        return self.get_at_index(self.get_index(id))

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

    def update_by_id(self, id: int, field: str, value):
        index = self.get_index(id)
        if index is None:
            return

        self.df_sbar[index,field] = value

    def update(self, with_columns):
        self.df_sbar = self.df_sbar.with_columns(with_columns)

    def get_dataframe(self):
        return self.df_sbar
