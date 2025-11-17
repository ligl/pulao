from typing import Any

from pulao.events import Observable
import polars as pl

from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPoint
from ..sbar import SBarManager, SBar


class _CBar:
    start_index: int  # 合并k线的开始索引（CBarManager）
    end_index: int  # 合并k线的结束索引
    high_price: float  # 合并后的最高价
    low_price: float  # 合并后的最低价
    swing_point: SwingPoint # 波段高低点标识


class SwingManager(Observable):
    sbar_manager: SBarManager()
    df: pl.DataFrame  # 包含合并后的k线列表

    def __init__(self, sbar_manager: SBarManager):
        super().__init__()
        schema = {
            "start_index": int,
            "end_index": int,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "swing_point": pl.Utf8,  # 波段高低点标记
        }
        self.df = pl.DataFrame(schema=schema)
        self.sbar_manager = sbar_manager
        self.sbar_manager.subscribe(self._on_sbar_created)

    def _on_sbar_created(self, event: EventType, payload: Any):
        self.detect(payload)
    def append(self, swing: Swing):
        self.notify(EventType.SWING_CHANGED, swing)

    def detect(self, sbar: SBar = None):
        # 波段检测识别
        # 1. K线包含处理
        self._agg_bar(sbar)
        # 2. 波段点识别
        self._detect_swing_point(sbar)
        # 3. 给SBar标注
        self._mark_swing_point()
        #self.notify(EventType.SWING_CHANGED, self)

    def _agg_bar(self, sbar: SBar):
        # 对传入sbar做K线包含处理
        index = self.df.height - 1
        cbar_df = self.df.slice(index, 2)
        # 包含合并处理逻辑
        # cbar_df中：
        # 第1行：合并时定方向用的bar
        # 第2行：与sbar做比较，判断是否需要合并
        high_price = 0
        low_price = 0
        start_index = 0
        end_index = 0
        if cbar_df.height == 2:  # 已有构造数据列表
            row_direction = cbar_df.row(0, named=True)
            row_compare = cbar_df.row(1, named=True)

            if row_compare["high_price"] > row_direction["high_price"]:  # 向上
                direction = SwingDirection.UP

            elif row_compare["low_price"] < row_direction["low_price"]:  # 向下
                direction = SwingDirection.DOWN
            else:
                # 不应该执行此处代码，如果执行，说明之前的数据有问题！！！
                direction = SwingDirection.NONE

            if (
                row_compare["high_price"] > sbar.high_price
                and row_compare["low_price"] < sbar.low_price
            ):  # 内包，即row_compare包含sbar
                start_index = row_compare["start_index"]
                end_index = sbar.index
                if direction == SwingDirection.UP:
                    # 方向向上，取高中高、低中高
                    high_price = row_compare["high_price"]
                    low_price = sbar.low_price
                else:
                    # 方向向下，取高中低、低中低
                    high_price = sbar.high_price
                    low_price = row_compare["low_price"]
            elif (
                row_compare["high_price"] < sbar.high_price
                and row_compare["low_price"] > sbar.low_price
            ):  # 外包，即sbar包含row_compare
                start_index = row_compare["start_index"]
                end_index = sbar.index
                if direction == SwingDirection.UP:
                    # 方向向上，取高中高、低中高
                    high_price = sbar.high_price
                    low_price = row_compare["low_price"]
                else:
                    # 方向向下，取高中低、低中低
                    high_price = row_compare["high_price"]
                    low_price = sbar.low_price

        elif cbar_df.height == 1:  # sbar为第2根
            # 丢弃被包含的bar
            row_compare = cbar_df.row(0, named=True)
            if (
                row_compare["high_price"] > sbar.high_price
                and row_compare["low_price"] < sbar.low_price
            ):  # 内包，即row_compare包含sbar
                high_price = row_compare["high_price"]
                low_price = row_compare["low_price"]
                start_index = row_compare["start_index"]
                end_index = sbar.index
            elif (
                row_compare["high_price"] < sbar.high_price
                and row_compare["low_price"] > sbar.low_price
            ):  # 外包，即sbar包含row_compare
                high_price = sbar.high_price
                low_price = sbar.low_price
                start_index = row_compare["start_index"]
                end_index = sbar.index
        else:  # 尚未构造数据，sbar为第1根
            # 直接使用sbar

            pass
        if high_price == 0 and low_price == 0: # 没有包含关系
            high_price = sbar.high_price
            low_price = sbar.low_price
            start_index = sbar.index
            end_index = sbar.index
        else:  # 有包含关系，
            # 1. 把row_compare删除
            self.df = self.df.filter(pl.arange(0, self.df.height) != index)
        # 2. 增加sbar
        row = {
            "start_index": start_index,
            "end_index": end_index,
            "high_price": high_price,
            "low_price": low_price,
            "swing_point":""
        }
        self.df = self.df.vstack(
            pl.DataFrame(
                [[row[col] for col in self.df.columns]],
                schema=self.df.schema,
                orient="row",
            )
        )  # append row

    def _detect_swing_point(self, sbar: SBar = None):
        # 波段点识别
        pass

    def _mark_swing_point(self, sbar: SBar = None):
        # 给SBar标注
        pass
