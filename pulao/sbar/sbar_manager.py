from pulao.events import Observable
from pulao.indicator import IndicatorManager, EmaIndicator
from .sbar import SBar
import polars as pl


class SBarManager(Observable):
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
            "swing_point": pl.Utf8,  # 波段高低点标记
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

        row = sbar_to_row(sbar)

        self.df = self.df.vstack(
            pl.DataFrame(
                [[row[col] for col in self.df.columns]],
                schema=self.df.schema,
                orient="row",
            )
        )  # append row
        self.notify("sbar.created", sbar)
        return sbar.index

    def get_by_index(self, idx: int):
        return self.df[idx]

    def get_by_time(self, dt):
        return self.df.filter(pl.col("datetime") == dt)

    def get_range(self, start, end):
        return self.df.filter(
            (pl.col("datetime") >= start) & (pl.col("datetime") <= end)
        )

    def update_by_datetime(self, dt, field, value):
        """根据 datetime 更新某字段（高性能）"""
        self.df = self.df.with_columns(
            pl.when(pl.col("datetime") == dt)
            .then(pl.lit(value))
            .otherwise(pl.col(field))
            .alias(field)
        )

    def update_by_index(self, index, field, value):
        """根据 row index 更新某字段（高性能）"""
        self.df = self.df.with_columns(
            pl.when(pl.arange(0, self.df.height) == index)
            .then(pl.lit(value))
            .otherwise(pl.col(field))
            .alias(field)
        )

    def recalc_recent_swing_flags(self, lookback: int = 7):
        # 根据最近 lookback 根bar重新计算波段高低点
        # 修改SBar的 is_swing_high / is_swing_low 并触发事件
        pass


def sbar_to_row(bar: SBar) -> dict:
    return {
        "symbol": bar.symbol,
        "exchange": str(bar.exchange),
        "interval": str(bar.interval),
        "datetime": bar.datetime,
        "volume": bar.volume,
        "open_interest": bar.open_interest,
        "open_price": bar.open_price,
        "high_price": bar.high_price,
        "low_price": bar.low_price,
        "close_price": bar.close_price,
        "swing_point": str(bar.swing_point),
        "ema_20": float(bar.ema_20),
        "ema_60": float(bar.ema_60),
    }
