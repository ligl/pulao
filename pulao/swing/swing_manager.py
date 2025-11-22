from typing import Any, Tuple
from pulao.events import Observable
import polars as pl

from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPointType, SwingPointLevel, Const
from ..bar import CBarManager


class SwingManager(Observable):
    cbar_manager: CBarManager

    def __init__(self, cbar_manager: CBarManager):
        super().__init__()
        self.cbar_manager = cbar_manager
        self.cbar_manager.subscribe(self._on_sbar_created)

    def _on_sbar_created(self, event: EventType, payload: Any):
        self.detect()

    def detect(self):
        # 波段检测识别
        pass

    def get_swing(self, index: int = None) -> Swing | None:
        """
        获取指定波段
        :param index: 指定index开始的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        if index is None:
            start_index = self.get_current_swing_start_index()
            end_index = self.cbar_manager.df_cbar.height - 1
        else:
            start_index = index
            start_swing_point_type = (
                self.cbar_manager.df_cbar.filter(pl.col("index") == start_index)
                .select(pl.col("swing_point_type").last())
                .item()
            )
            if start_swing_point_type == SwingPointType.HIGH:
                end_swing_point_type = SwingPointType.LOW
            elif start_swing_point_type == SwingPointType.LOW:
                end_swing_point_type = SwingPointType.HIGH
            else:  # 给定index不是一个波段的起点
                return None
            end_index = (
                self.cbar_manager.df_cbar.slice(start_index, Const.LOOKBACK_LIMIT)
                .filter(
                    (pl.col("swing_point_type") == end_swing_point_type)
                    & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
                )
                .select(pl.col("index").last())
                .item()
            )
            end_index = (
                self.cbar_manager.df_cbar.height - 1 if not end_index else end_index
            )  # 如果没有查到波段终点，说明波段并未结束
        current_swing_df = self.cbar_manager.df_cbar.slice(start_index, end_index - start_index + 1)
        swing = _parse_swing(current_swing_df)
        return swing

    def prev_opposite_swing(self, index: int = None) -> Swing | None:
        """
        前一个与指定波段相反方向的波段
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        current_swing = self.get_swing(index)
        if current_swing is None:
            return None
        if current_swing.direction == SwingDirection.UP:
            prev_opposite_swing_point_type = SwingPointType.HIGH
        else:
            prev_opposite_swing_point_type = SwingPointType.LOW

        prev_opposite_swing_end_index = current_swing.index
        slice_index = (
            prev_opposite_swing_end_index - Const.LOOKBACK_LIMIT
            if prev_opposite_swing_end_index > Const.LOOKBACK_LIMIT
            else 0
        )
        prev_opposite_swing_start_index = (
            self.cbar_manager.df_cbar.slice(
                slice_index, prev_opposite_swing_end_index - slice_index + 1
            )
            .filter(
                (pl.col("swing_point_type") == prev_opposite_swing_point_type)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )
            .select(pl.col("index").last())
            .item()
        )
        if not prev_opposite_swing_start_index:
            return None
        prev_opposite_swing_df = self.cbar_manager.df_cbar.slice(
            prev_opposite_swing_start_index,
            prev_opposite_swing_end_index - prev_opposite_swing_start_index + 1,
        )
        swing = _parse_swing(prev_opposite_swing_df)
        return swing

    def prev_same_swing(self, index: int = None) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        prev_opposite_swing = self.prev_opposite_swing(index)
        if prev_opposite_swing is None:
            return None
        prev_same_swing_end_index = prev_opposite_swing.index

        if prev_opposite_swing.direction == SwingDirection.UP:
            prev_same_swing_point_type = SwingPointType.HIGH
        else:
            prev_same_swing_point_type = SwingPointType.LOW

        slice_index = (
            prev_same_swing_end_index - Const.LOOKBACK_LIMIT
            if prev_same_swing_end_index > Const.LOOKBACK_LIMIT
            else 0
        )
        prev_same_swing_start_index = (
            self.cbar_manager.df_cbar.slice(slice_index, prev_same_swing_end_index - slice_index + 1)
            .filter(
                (pl.col("swing_point_type") == prev_same_swing_point_type)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )
            .select(pl.col("index").last())
            .item()
        )
        if not prev_same_swing_start_index:
            return None
        prev_same_swing_df = self.cbar_manager.df_cbar.slice(
            prev_same_swing_start_index,
            prev_same_swing_end_index - prev_same_swing_start_index + 1,
        )
        swing = _parse_swing(prev_same_swing_df)
        return swing

    def next_opposite_swing(self, index: int = None) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        raise NotImplementedError("未实现")

    def next_same_swing(self, index: int = None) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        raise NotImplementedError("未实现")

    def prev_swing(self, index: int = None) -> Swing | None:
        """
        查指定波段的前一个波段（与prev_opposite_swing等效）
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        return self.prev_opposite_swing(index)

    def next_swing(self, index: int = None) -> Swing | None:
        """
        查指定波段的后一个波段（与next_opposite_swing等效）
        :param index: 指定index所在的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        return self.next_opposite_swing(index)

    def get_swing_list(self, start_index: int, end_index: int):
        """
        获取波段列表
        :param start_index:
        :param end_index:
        :return:
        """
        return self.cbar_manager.df_cbar.slice(start_index, end_index - start_index + 1).filter(
            (pl.col("swing_point_type") != SwingPointType.NONE)
            & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
        )

    def get_bar_list(self, start_index: int, end_index: int, origin_bar=False):
        """
        获取指定索引段的bar列表
        :param start_index: swing.start_index，即cbar_list.index
        :param end_index:  swing.end_index ，即cbar_list.index
        :param origin_bar: 是否返回原始k线，True:返回sbar_list，False:返回cbar_list
        :return: bar list
        """
        raise NotImplementedError("未实现")

    def get_current_swing_start_index(self) -> int:
        """
        取当前波段的开始索引
        :return:
        """
        # 取当前波段的开始索引
        start_index = (
            self.cbar_manager.df_cbar.filter(
                (pl.col("swing_point_type") != SwingPointType.NONE)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )
            .select(pl.col("index").last())
            .item()
        )
        return start_index

def _parse_swing(cbar_df: pl.DataFrame) -> Swing | None:
    """
    解析Swing
    :param cbar_df: 包含波段高低点的数据集，第一行为波段起点，最后一行为波段终点
    :return: Swing | None
    """
    if cbar_df.is_empty():
        return None

    start_row = cbar_df.row(0, named=True)
    end_row = cbar_df.tail(1).row(0, named=True)

    swing = Swing()

    swing.index = start_row["index"]

    swing.start_index = start_row["index"]
    swing.end_index = end_row["index"]

    swing.start_index_bar = start_row["start_index"]
    swing.end_index_bar = end_row["end_index"]

    if start_row["swing_point_type"] == SwingPointType.LOW:
        swing.direction = SwingDirection.UP
    elif start_row["swing_point_type"] == SwingPointType.HIGH:
        swing.direction = SwingDirection.DOWN
    else:
        swing.direction = SwingDirection.NONE

    swing.high_price = max(start_row["high_price"], end_row["high_price"])
    swing.low_price = min(start_row["low_price"], end_row["low_price"])

    # 判断波段是否完成
    if (
        start_row["swing_point_level"] == end_row["swing_point_level"]
        and start_row["swing_point_type"] != end_row["swing_point_type"]
        and start_row["swing_point_type"] != SwingPointType.NONE
        and end_row["swing_point_type"] != SwingPointType.NONE
    ):
        swing.is_completed = True
    else:
        swing.is_completed = False
    return swing
