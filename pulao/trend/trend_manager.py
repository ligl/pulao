from typing import Any, List

import polars as pl
from datetime import datetime as Datetime

from pulao.events import Observable
from .trend import Trend
from ..bar import  Fractal, CBar
from ..constant import (
    EventType,
    Direction,
    FractalType, Const,
)
from ..logging import logger
from ..swing import SwingManager, Swing
from ..utils import IDGenerator


class TrendManager(Observable):
    def __init__(self, swing_manager: SwingManager):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "start_id": pl.UInt64, # df_cbar id
            "end_id": pl.UInt64,  # 如果是active trend，end_id = 最新k线
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "direction": pl.Int8,
            "is_completed": pl.Boolean,  # 还未被确认的趋势，即正在进行中的趋势，在实时行情中尚未被确认
            "created_at": pl.Datetime("ms"),
        }
        self.df_trend: pl.DataFrame = pl.DataFrame(schema=schema)
        self.swing_manager: SwingManager = swing_manager
        self.swing_manager.subscribe(self._on_swing_changed)
        self.id_gen = IDGenerator()

    def _on_swing_changed(self, event: EventType, payload: Any):
        # 趋势检测识别
        # logger.debug("_on_cbar_created", payload=payload)
        # if payload["backtrack_id"] is None:
        #     self._build_trend()
        # else:
        #     self._clean_reset(payload["backtrack_id"])
        #     self._backtrack_replay(payload["backtrack_id"])
        self._build_trend()

    def _clean_reset(self, traceback_id: int):
        # 1. 清理df_trend
        df = self.df_trend.filter(
            (pl.col("start_id") <= traceback_id) & (traceback_id <= pl.col("end_id")))
        if df.is_empty():
            return
        # 只有一种情况会出现两条数据，即traceback_id是一个趋势的终点，同时又是另一个趋势的起点
        first_trend = Trend(**df.row(0, named=True))
        first_trend_index = self.get_index(first_trend.id)
        if df.height > 1:
            # 删除traceback_id之后的数据
            logger.debug("_clean_trend 删除traceback_id之后的数据", traceback_id=traceback_id,
                         first_trend=first_trend, first_trend_index=first_trend_index)
            self.df_trend = self.df_trend.slice(0, first_trend_index + 1)
        if first_trend.start_id == traceback_id:  # 说明只有一个趋势的时候
            logger.debug("_clean_trend 清空趋势，重新构建", first_trend=first_trend,
                         first_trend_index=first_trend_index)
            self.df_trend = self.df_trend.slice(0, first_trend_index)
        else:
            # 在趋势的中间
            first_trend.is_completed = False
            cbar = self.swing_manager.get_nearest_swing(traceback_id, -1)
            first_trend.end_id = cbar.id if cbar else None
            self._update_active_trend(id=first_trend.id, direction=first_trend.direction,
                              start_id=first_trend.start_id, end_id=first_trend.end_id,
                              high_price=first_trend.high_price, low_price=first_trend.low_price,
                              is_completed=first_trend.is_completed)

    def _backtrack_replay(self, backtrack_id:int = None):
        """
        查找要从哪个swing开始回放处理，取值min(backtrack_id, trend.end_id)
        """
        # 2. 获取需要处理的swing[backtrack_id, last_id]，进行回放
        swing_list = self.swing_manager.get_nearest_swing(backtrack_id)
        if swing_list is None:
            return
        for swing in swing_list:
            self._build_trend(swing)

    def _build_trend(self, swing: Swing = None):
        """
        构建趋势
        """
        #
        # 趋势
        # 定义：由波段的高低间关系构成
        # 判定标准:
        # 1. 由最近的4个波段高低点，2高2低构建最近的趋势类型
        # 2. 高点抬高，低点抬高 → 上涨趋势
        # 3. 高点降低，低点降低 → 下降趋势
        # 4. 高点和低点重叠错落，无明显趋势 → 横盘震荡区间
        # 执行流程：
        # 1. 获取最近已完成的3个波段，判断趋势类型
        # 3. 向前延伸寻找趋势的起点，是延续前一趋势，还是开启新的趋势
        # Q&A：
        # 1. 起点选择的问题？
        #


    def _del_active_trend(self):
        active_trend = self.get_active_trend()
        if active_trend:
            self.df_trend = self.df_trend.slice(
                0, self.df_trend.height - 1
            )  # 删除未完成的趋势

    def _append_trend(
        self,
        direction: Direction,
        start_id: int,
        end_id: int,
        high_price: float,
        low_price: float,
        is_completed: bool
    ):
        new_trend = {
            "id": self.id_gen.get_id(),
            "direction": direction.value,
            "start_id": start_id,
            "end_id": end_id,
            "high_price": high_price,
            "low_price": low_price,
            "is_completed": is_completed,
            "created_at": Datetime.now(),
        }
        if is_completed:
            start_fractal = self.swing_manager.get_fractal(start_id)
            end_fractal = self.swing_manager.get_fractal(end_id)
            if not start_fractal or not end_fractal:
                logger.error(
                    "趋势断定无效",
                    trend=new_trend,
                    start_fractal=start_fractal,
                    end_fractal=end_fractal,
                )
                raise AssertionError("趋势断定无效")
        self.df_trend = self.df_trend.vstack(
            pl.DataFrame([new_trend], schema=self.df_trend.schema)
        )

    def _update_active_trend(
        self,
        id: int,
        direction: Direction,
        start_id: int,
        end_id: int,
        high_price: float,
        low_price: float,
        is_completed: bool,
    ):
        """
        更新逻辑：先删除旧数据，再添加新数据，每条数据的id都不同
        """
        # 先删除再添加
        if self.df_trend.height > 0:
            self.df_trend = self.df_trend.slice(
                0, self.df_trend.height - 1
            )  # 删除原active trend，即最后一行
        self._append_trend(
            direction, start_id, end_id, high_price, low_price, is_completed
        )

    def _determine_trend(
        self,
        start_swing: Fractal,
        end_swing: Fractal,
        active_trend: Trend,
        prev_trend: Trend = None,
    ) -> bool:
        """
        判定两个分形是否能够组成笔
        """

        return False

    def get_fractal(self, cbar_id: int) -> Fractal:
        return self.swing_manager.get_fractal(cbar_id)

    def get_active_trend(self) -> Trend | None:
        """
        获取未完成的趋势
        :return: Trend | None
        """
        last_trend = self.get_trend()
        if not last_trend:
            return None
        if last_trend.is_completed:
            # 最后一个趋势已经完成
            return None
        else:
            # 如果未完成，那么最后一个趋势就是active trend
            return last_trend

    def get_index(self, id: int) -> int:
        return self.df_trend.select(pl.col("id").search_sorted(id)).item()

    def get_trend(self, id: int = None, is_completed:bool = None) -> Trend | None:
        """
        获取指定趋势
        :param id: 指定id的趋势，如果没有指定，获取最新趋势
        :param is_completed: None：不限制
        :return: Trend | None
        """
        if id is None:
            index = self.df_trend.height - 1 # 取最后一条
        else:
            index = self.get_index(id)

        if index is None or index < 0 or index > self.df_trend.height -1:
            return None

        trend =  Trend(**self.df_trend.row(index, named=True))
        # 有没有指定id
        if id:
            if is_completed is None:
                return trend
            else:
                return trend if trend.is_completed == is_completed else None
        else:
            # 没有指定id
            if is_completed is None: # a. 对状态没有要求
                return trend
            else:
                if trend.is_completed == is_completed: # b. 要求的状态正好与最后一条吻合
                    return trend
                else: # c. 要求的状态与最后一条不吻合，查找最新的一条满足条件的数据
                    df = self.df_trend.tail(Const.LOOKBACK_LIMIT).filter(pl.col("is_completed") == is_completed).tail(1)
                    if df.is_empty():
                        return None
                    return Trend(**df.row(0, named=True))

    def get_trend_by_index(self, index: int) -> Trend | None:
        """
        通过索引获取趋势
        :param index: 索引值
        :return: Trend | None
        """
        if index is None or index <= 0 or index >= self.df_trend.height - 1:
            return None
        return Trend(**self.df_trend.row(index - 1, named=True))

    def prev_opposite_trend(self, id: int) -> Trend | None:
        """
        前一个与指定趋势相反方向的趋势
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_trend_by_index(index - 1)

    def prev_same_trend(self, id: int) -> Trend | None:
        """
        前一个与指定趋势相同方向的趋势
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_trend_by_index(index - 2)

    def next_opposite_trend(self, id: int) -> Trend | None:
        """
        后一个与指定趋势相反方向的趋势
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_trend_by_index(index + 1)

    def next_same_trend(self, id: int) -> Trend | None:
        """
        后一个与指定趋势相同方向的趋势
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_trend_by_index(index + 2)

    def prev_trend(self, id: int) -> Trend | None:
        """
        查指定趋势的前一个趋势（与prev_opposite_trend等效）
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        return self.prev_opposite_trend(id)

    def next_trend(self, id: int) -> Trend | None:
        """
        查指定趋势的后一个趋势（与next_opposite_trend等效）
        :param id: 指定id所在的趋势
        :return: Trend | None
        """
        return self.next_opposite_trend(id)

    def get_trend_list(
        self, start_id: int, end_id: int, include_active: bool = True
    ) -> List[Trend] | None:
        """
        获取[start_id,end_id]之间的趋势列表
        :param start_id:
        :param end_id:
        :param include_active: 是否包含active trend
        :return:
        """
        start_index = self.get_index(start_id)
        end_index = self.get_index(end_id)
        if start_index is None or end_index is None:
            return None
        if start_index > end_index:  # 交换
            start_index, end_index = end_index, start_index

        df = self.df_trend.slice(start_index, end_index - start_index + 1)
        if not include_active:
            df = df.filter((pl.col("is_completed") == True))
        if df.is_empty():
            return None
        return [Trend(**row) for row in df.rows(named=True)]

    def get_swing_list(self, trend:Trend) -> List[Swing] | None:
        return self.swing_manager.get_swing_list(trend.start_id, trend.end_id)

