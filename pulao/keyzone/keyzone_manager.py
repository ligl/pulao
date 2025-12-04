from typing import Any

from pulao.constant import EventType, Timeframe
from pulao.events import Observable
from pulao.logging import logger
from pulao.mtc import MultiTimeframeContext
from pulao.trend import TrendManager, Trend
import polars as pl

from pulao.utils import IDGenerator


class KeyZoneManager(Observable):
    def __init__(self, mtc: MultiTimeframeContext):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "origin_type": pl.Utf8,
            "timeframe": pl.Utf8,
            "orientation":pl.Int8,
            "sbar_start_id": pl.UInt64,  # df_sbar id
            "sbar_end_id": pl.UInt64,

            "upper": pl.Float32,
            "lower": pl.Float32,
            "trendline_slope":pl.Float32,
            "trendline_intercept":pl.Float32,
            "channel_line_slope":pl.Float32,
            "channel_line_intercept":pl.Float32,

            "touch_count": pl.UInt32,
            "last_touch_id":pl.UInt64,

            "is_completed": pl.Boolean,  # 还未被确认的KeyZone，即正在进行中的KeyZone，在实时行情中尚未被确认
            "created_at": pl.Datetime("ms"),
        }
        self.df_keyzone: pl.DataFrame = pl.DataFrame(schema=schema)
        self.mtc: MultiTimeframeContext = mtc
        self.mtc.subscribe(self._on_mtc)
        self.id_gen = IDGenerator(worker_id=5)

    def _on_mtc(self, timeframe:Timeframe, event: EventType, payload: Any):
        logger.debug(f"_on_mtc: {timeframe}, {event}, {payload}")

    def write_parquet(self):
        # TODO 实时行情不能这么做，需要考虑性能影响
        self.df_keyzone.write_parquet(
            "./keyzone_data.parquet",
            compression="zstd",
            compression_level=3,
            statistics=False
        )

    def read_parquet(self):
        self.df_keyzone = pl.read_parquet("./keyzone_data.parquet")
        return self.df_keyzone
