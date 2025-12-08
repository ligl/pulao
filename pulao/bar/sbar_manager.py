from typing import List

from polars import Datetime

from pulao.events import Observable
from pulao.indicator import IndicatorManager, EmaIndicator
from .sbar import SBar
import polars as pl

from ..constant import EventType, Timeframe, Const
from ..utils import IDGenerator
from datetime import datetime as Datetime


class SBarManager(Observable):
    """
    管理缓存bar数据，计算指标
    """

    def __init__(self, symbol: str, timeframe: Timeframe):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "datetime": pl.Datetime,
            "symbol": pl.Utf8,
            "exchange": pl.Utf8,
            "timeframe": pl.Utf8,
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

        self.symbol = symbol
        self.timeframe: Timeframe = timeframe
        self.indicator_manager: IndicatorManager = IndicatorManager()
        self.indicator_manager.register(EmaIndicator(20))
        self.indicator_manager.register(EmaIndicator(60))
        self.id_gen: IDGenerator = IDGenerator(worker_id=0)

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
            "timeframe": sbar.timeframe.value,
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
        self.write_parquet()
        self.notify(self.timeframe, EventType.SBAR_CREATED, sbar=sbar)
        return sbar.id

    def get_index(self, id: int) -> int | None:
        idx = self.df_sbar.select(pl.col("id").search_sorted(id)).item()
        if idx is None or self.df_sbar["id"][idx] != id:
            return None
        else:
            return idx

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

    def get_limit_sbar(self, start_id: int, end_id: int, arg=str) -> SBar | None:
        """
        获取一段区间[start_id, end_id]中的最高价或最低价，即max(high_price)或min(low_price)
        :param start_id:
        :param end_id:
        :param arg: max or min
        :return:
        """
        if arg not in ["max", "min"]:
            return None
        start_index = self.get_index(start_id)
        end_index = self.get_index(end_id)
        if start_index is None or end_index is None:
            return None
        if start_index > end_index:  # 交换
            start_index, end_index = end_index, start_index

        df = self.df_sbar.slice(start_index, end_index - start_index + 1)
        if df.is_empty():
            return None
        if arg == "max":
            index = df["high_price"].arg_max()
        else:
            index = df["low_price"].arg_min()
        return SBar(**df.row(index, named=True))

    def get_limit_sbar_id(self, start_id: int, end_id: int, arg=str) -> int | None:
        sbar = self.get_limit_sbar(start_id, end_id, arg)
        if sbar is None:
            return None
        return sbar.id

    @property
    def total_count(self):
        return self.df_sbar.height

    def get_last_sbar(self, length: int = 1):
        return self.df_sbar.slice(-length)

    def get_around_sbar(
        self, pivot_id: int, length: int, ret_df: bool = False
    ) -> List[SBar] | None | pl.DataFrame:
        """
        获取在pivot_id周围的sbar list
        :param pivot_id: 中轴点
        :param length: 左右各多少根
        :param ret_df: 是否返回原生pl.DataFrame
        :return:
        """
        idx = self.get_index(pivot_id)
        start_index = idx - length
        end_index = idx + length

        df = self.df_sbar.slice(start_index, end_index - start_index + 1)
        if ret_df:
            return df

        if df.is_empty():
            return None
        return [SBar(**row) for row in df.rows(named=True)]

    def update_by_id(self, id: int, field: str, value):
        index = self.get_index(id)
        if index is None:
            return

        self.df_sbar[index, field] = value

    def update(self, with_columns):
        self.df_sbar = self.df_sbar.with_columns(with_columns)

    def get_dataframe(self):
        return self.df_sbar

    def write_parquet(self):
        # TODO 实时行情不能这么做，需要考虑性能影响
        self.df_sbar.write_parquet(
            Const.PARQUET_PATH.format(
                symbol=self.symbol, filename=f"sbar_{self.timeframe}"
            ),
            compression="zstd",
            compression_level=3,
            statistics=False,
            mkdir=True,
        )

    def read_parquet(self):
        self.df_sbar = pl.read_parquet(
            Const.PARQUET_PATH.format(
                symbol=self.symbol, filename=f"sbar_{self.timeframe}"
            )
        )
        return self.df_sbar

    def stat(self, start_id:int, end_id:int) -> dict|None:
        start_idx = self.get_index(start_id)
        end_idx = self.get_index(end_id)
        if start_idx is None or end_idx is None:
            return None
        if start_idx > end_idx:
            start_idx, end_idx = end_idx, start_idx

        df = self.df_sbar.slice(start_idx, end_idx - start_idx + 1)
        if df.is_empty():
            return None

        start_oi = df.select(pl.col("open_interest").first()).item()
        end_oi = df.select(pl.col("open_interest").last()).item()
        volume = df.select(pl.col("volume").sum()).item()
        return dict(span=df.height, volume=volume, start_oi=start_oi, end_oi=end_oi)
