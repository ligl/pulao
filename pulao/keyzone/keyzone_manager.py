from typing import \
    Any, \
    List

from pulao.constant import \
    EventType, \
    Timeframe, \
    KeyZoneOrigin, \
    Const
from pulao.events import Observable
from pulao.keyzone.builder import SwingKeyZoneBuilder, TrendKeyZoneBuilder
from pulao.keyzone.builder_factory import KeyZoneFactory
from pulao.keyzone.keyzone import KeyZone
from pulao.logging import get_logger
from pulao.mtc.mtc import MultiTimeframeContext
import polars as pl
from datetime import datetime as Datetime
from pulao.utils import IDGenerator

logger = get_logger(__name__)

# 注册各类origin_type构建器
KeyZoneFactory.register(SwingKeyZoneBuilder)
KeyZoneFactory.register(TrendKeyZoneBuilder)


class KeyZoneManager(Observable):
    def __init__(self, mtc: MultiTimeframeContext):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "timeframe": pl.Utf8,
            "origin_type": pl.Utf8,
            "orientation": pl.Int8,
            "upper": pl.Float32,
            "lower": pl.Float32,
            "trendline_slope": pl.Float32,
            "trendline_intercept": pl.Float32,
            "channel_line_slope": pl.Float32,
            "channel_line_intercept": pl.Float32,
            "touch_count": pl.UInt32,
            "last_touch_id": pl.UInt64,
            "sbar_start_id": pl.UInt64,
            "sbar_end_id": pl.UInt64,
            "created_at": pl.Datetime("ms"),
        }
        self.df_keyzone: pl.DataFrame = pl.DataFrame(schema=schema)
        self.mtc: MultiTimeframeContext = mtc
        self.mtc.subscribe(self._on_new_bar, EventType.MTC_NEW_BAR)
        self.id_gen = IDGenerator(worker_id=5)

    def _on_new_bar(self, timeframe: Timeframe, event: EventType, payload: Any):
        # 让各builder构造keyzone
        # sbar_list = self.mtc.get_sbar_window(200, timeframe)
        # cbar_list = self.mtc.get_cbar_window(200, timeframe)

        swing_keyzone_list = KeyZoneFactory.create(
            self.mtc,
            KeyZoneOrigin.SWING,
            timeframe
        ).build()

        trend_keyzone_list = KeyZoneFactory.create(
            self.mtc, KeyZoneOrigin.TREND, timeframe
        ).build()


        # 添加或更新KeyZone
        self._clear_keyzone(timeframe)
        self._append_keyzone(swing_keyzone_list + trend_keyzone_list)
        logger.debug("_on_new_bar in mtc", swing_keyzone_list=swing_keyzone_list, trend_keyzone_list=trend_keyzone_list)
        self.write_parquet()

    def _clear_keyzone(self, timeframe: Timeframe):
        self.df_keyzone = self.df_keyzone.filter(~(pl.col("timeframe")==timeframe))

    def _append_keyzone(self, keyzone_list: List[KeyZone]):

        rows =[]
        for keyzone in keyzone_list:
            keyzone_dict = vars(
                keyzone)
            keyzone_dict["id"] = self.id_gen.get_id()
            keyzone_dict["timeframe"] = keyzone_dict["timeframe"].value
            keyzone_dict["origin_type"] = keyzone_dict["origin_type"].value
            keyzone_dict["orientation"] = keyzone_dict["orientation"].value
            keyzone_dict["created_at"] = Datetime.now()
            rows.append(keyzone_dict)
        self.df_keyzone = self.df_keyzone.vstack(
            pl.DataFrame(
                rows,
                schema=self.df_keyzone.schema,
                orient="row",
            )
        )  # append row

    def write_parquet(self):
        # TODO 实时行情不能这么做，需要考虑性能影响
        self.df_keyzone.write_parquet(
            Const.PARQUET_PATH.format(symbol=self.mtc.symbol, filename=f"keyzone"),
            compression="zstd",
            compression_level=3,
            statistics=False,
            mkdir=True,
        )

    def read_parquet(self):
        self.df_keyzone = pl.read_parquet(Const.PARQUET_PATH.format(symbol=self.mtc.symbol, filename=f"keyzone"))
        return self.df_keyzone
