import copy
from typing import Any, List

import polars as pl
from datetime import datetime as Datetime

from pulao.events import Observable
from .trend import Trend
from ..bar import Fractal, CBar
from ..constant import (
    EventType,
    Direction,
    FractalType,
    Const,
)
from ..logging import logger
from ..swing import SwingManager, Swing
from ..utils import IDGenerator


class TrendManager(Observable):
    def __init__(self, swing_manager: SwingManager):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "swing_start_id": pl.UInt64,  # df_cbar id
            "swing_end_id": pl.UInt64,  # 如果是active trend，end_id = 最新k线
            "sbar_start_id": pl.UInt64,  # df_sbar id
            "sbar_end_id": pl.UInt64,
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
        self.sfs_trend = [] # 趋势的特征序列
        self.sfs_opposite = [] # 反向趋势的特征序列

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
            (pl.col("swing_start_id") <= traceback_id) & (traceback_id <= pl.col("swing_end_id"))
        )
        if df.is_empty():
            return
        # 只有一种情况会出现两条数据，即traceback_id是一个趋势的终点，同时又是另一个趋势的起点
        first_trend = Trend(**df.row(0, named=True))
        first_trend_index = self.get_index(first_trend.id)
        if df.height > 1:
            # 删除traceback_id之后的数据
            logger.debug(
                "_clean_trend 删除traceback_id之后的数据",
                traceback_id=traceback_id,
                first_trend=first_trend,
                first_trend_index=first_trend_index,
            )
            self.df_trend = self.df_trend.slice(0, first_trend_index + 1)
        if first_trend.swing_start_id == traceback_id:  # 说明只有一个趋势的时候
            logger.debug(
                "_clean_trend 清空趋势，重新构建",
                first_trend=first_trend,
                first_trend_index=first_trend_index,
            )
            self.df_trend = self.df_trend.slice(0, first_trend_index)
        else:
            # 在趋势的中间
            first_trend.is_completed = False
            swing = self.swing_manager.get_nearest_swing(traceback_id, -1)
            if swing:
                first_trend.swing_end_id = swing.id
                first_trend.sbar_start_id = swing.sbar_start_id
                first_trend.sbar_end_id = swing.sbar_end_id
            self._update_active_trend(first_trend)

    def _backtrack_replay(self, backtrack_id: int = None):
        """
        查找要从哪个swing开始回放处理，取值min(backtrack_id, trend.end_id)
        """
        # 2. 获取需要处理的swing[backtrack_id, last_id]，进行回放
        swing_list = self.swing_manager.get_nearest_swing(backtrack_id)
        if swing_list is None:
            return
        for swing in swing_list:
            self._build_trend(swing)

    def _build_trend(self, curr_swing: Swing = None):
        """
        构建趋势
        """
        # 情况1. 首次创建
        if self.df_trend.is_empty():
            # 第一次创建趋势，需要有3个连续有重叠的波段组成
            swing_list = self.swing_manager.get_last_swing(3)
            if not swing_list or len(swing_list) != 3:
                logger.debug(
                    "波段数量不足3个，跳过", swing_list=swing_list
                )
                return
            swing_start, swing_middle, swing_end = swing_list
            if swing_start.overlap(swing_middle, swing_end):
                # 有重叠
                # 检查波段方向是否符合要求：上下上或下上下
                if (
                    not swing_start.direction
                        == swing_middle.opposite_direction
                        == swing_end.direction
                ):
                    return
                # 检查波段的高低点是否符合要求
                if swing_start.direction == Direction.DOWN:
                    # 下降波段
                    if (
                        swing_start.high_price <= swing_end.high_price
                        or swing_start.low_price <= swing_end.low_price
                    ):
                        return
                else:  # 上升波段
                    if (
                        swing_start.high_price >= swing_end.high_price
                        or swing_start.low_price >= swing_end.low_price
                    ):
                        return

                # 第一个波段的起点为趋势的起点，最后一个波段的终点为趋势临时的终点
                new_trend = Trend(
                    direction=swing_start.direction,
                    swing_start_id=swing_start.id,
                    swing_end_id=swing_end.id,
                    sbar_start_id=swing_start.sbar_start_id,
                    sbar_end_id=swing_end.sbar_end_id,
                    high_price=swing_end.high_price,
                    low_price=swing_start.low_price,
                    is_completed=False,
                )
                self._append_trend(new_trend)
                logger.debug(
                    "首次创建趋势", new_trend=new_trend,
                )
                self.sfs_trend.append(swing_middle)
            return  # 首次尝试构建结构

        # 已存在趋势，非首次构建
        active_trend = self.get_active_trend()
        last_swing = self.swing_manager.get_last_swing()

        # 情况2. 特征序列判断趋势转折点
        # 特征序列sfs(Structural Feature Sequence)
        # 上涨趋势由向下的波段组成特征序列，（下跌对称）
        # 上涨趋势只考察特征序列的顶分形，顶分形的顶就是趋势转折点（下跌对称）
        # 特征序列需要先按照K线合并的相同规则进行包含合并处理
        # 特征序列判断方法分为两种情况：
        # (以上涨为例)
        # 1. 分形第1根与第2根没有缺口，那么分形的顶点就是转折点
        # 2. 有缺口，那么需要以最高点为起点，建立新的特征序列，
        # 2.1 如果出现底分形，则最高点为趋势的转折点
        # 2.1 如果没有出现底分形，价格又超出趋势最高点，则延续原有趋势
        #

        if last_swing.direction != active_trend.direction:
            # 特征序列合并处理
            # 判断两根K线是否存在包含关系
            def is_inclusive(a_high, a_low, b_high, b_low):
                return (a_high >= b_high and a_low <= b_low) or (
                    a_high <= b_high and a_low >= b_low
                )

            tmp_swing = copy.deepcopy(last_swing)
            if self.sfs_trend:
                while True: # 循环往前处理包含
                    if not self.sfs_trend:
                        break
                    prev_sf:Swing = self.sfs_trend[-1]
                    if is_inclusive(prev_sf.high_price, prev_sf.low_price, tmp_swing.high_price, tmp_swing.low_price):
                        # 有包含关系
                        if active_trend.direction == Direction.UP:
                            tmp_swing.high_price = max(tmp_swing.high_price, prev_sf.high_price)
                            tmp_swing.low_price = max(tmp_swing.low_price, prev_sf.low_price)
                        else:
                            tmp_swing.high_price = min(tmp_swing.high_price, prev_sf.high_price)
                            tmp_swing.low_price = min(tmp_swing.low_price, prev_sf.low_price)
                        self.sfs_trend.pop()
                        logger.debug("特征序列发现包含关系",active_trend=active_trend,sfs=self.sfs_trend)
                    else:
                        break
            self.sfs_trend.append(tmp_swing) # 添加特征序列元素

        active_trend.swing_end_id = last_swing.id
        active_trend.high_price = max(last_swing.high_price, active_trend.high_price)
        active_trend.low_price = min(last_swing.low_price, active_trend.low_price)
        active_trend.sbar_end_id = last_swing.sbar_end_id

        # 用特征序列判断趋势是否终结
        if len(self.sfs_trend) < 3:
            # 说明不会有分形，延续趋势
            self._update_active_trend(active_trend)
            return
        # 最后3个元素是否组成分形，第1和第2个元素之间是否有缺口
        right_sf = self.sfs_trend[-1]
        mid_sf = self.sfs_trend[-2]
        left_sf = self.sfs_trend[-3]

        fractal_type = Fractal.verify(left_sf, mid_sf, right_sf)

        if fractal_type == FractalType.NONE:
            self._update_active_trend(active_trend)
            return

        has_gap = left_sf.high_price < mid_sf.low_price if fractal_type == FractalType.TOP else left_sf.low_price > mid_sf.high_price

        if has_gap:
            pass
        else:
            # 形成趋势转折点
            active_trend.is_completed = True
            active_trend = self._normal_trend(active_trend)
            self._update_active_trend(active_trend)
            logger.debug("趋势终止", active_trend = active_trend,)

            start_swing = self.swing_manager.next_swing(active_trend.swing_end_id)
            new_trend = Trend(
                direction=active_trend.opposite_direction,
                swing_start_id=start_swing.id,
                swing_end_id=last_swing.id,
                sbar_start_id=start_swing.sbar_start_id,
                sbar_end_id=last_swing.sbar_end_id,
                high_price= active_trend.high_price if active_trend.direction == Direction.UP else last_swing.high_price,
                low_price=last_swing.low_price if active_trend.direction == Direction.UP else active_trend.low_price,
                is_completed=False,
            )

            self._append_trend(new_trend)
            logger.debug("创建新趋势", new_trend=new_trend,
                         last_swing=last_swing)

            self.sfs_trend.clear()  # 清空特征序列

    def _normal_trend(self, trend: Trend)-> Trend:
        """
        标准化趋势，用趋势内的极值点作为趋势的起止点
        :param self:
        :param trend:
        :return:
        """
        swing = self.swing_manager.get_limit_swing(
            trend.swing_start_id,
            trend.swing_end_id,
            "min" if trend.direction == Direction.DOWN else "max",
            trend.direction)
        trend.swing_end_id = swing.id
        trend.sbar_end_id = swing.sbar_end_id
        trend.high_price = max(swing.high_price, trend.high_price)
        trend.low_price = min(swing.low_price, trend.low_price)
        return trend

    def _del_active_trend(self):
        active_trend = self.get_active_trend()
        if active_trend:
            self.df_trend = self.df_trend.slice(
                0, self.df_trend.height - 1
            )  # 删除未完成的趋势

    def _append_trend(self, trend: Trend):
        new_trend = {
            "id": self.id_gen.get_id(),
            "direction": trend.direction.value,
            "swing_start_id": trend.swing_start_id,
            "swing_end_id": trend.swing_end_id,
            "sbar_start_id": trend.sbar_start_id,
            "sbar_end_id": trend.sbar_end_id,
            "high_price": trend.high_price,
            "low_price": trend.low_price,
            "is_completed": trend.is_completed,
            "created_at": Datetime.now(),
        }

        self.df_trend = self.df_trend.vstack(
            pl.DataFrame([new_trend], schema=self.df_trend.schema)
        )

    def _update_active_trend(self, trend: Trend):
        """
        更新逻辑：先删除旧数据，再添加新数据，每条数据的id都不同
        """
        # 先删除再添加
        if self.df_trend.height > 0:
            self.df_trend = self.df_trend.slice(
                0, self.df_trend.height - 1
            )  # 删除原active swing，即最后一行
        self._append_trend(trend)

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

    def get_trend(self, id: int = None, is_completed: bool = None) -> Trend | None:
        """
        获取指定趋势
        :param id: 指定id的趋势，如果没有指定，获取最新趋势
        :param is_completed: None：不限制
        :return: Trend | None
        """
        if id is None:
            index = self.df_trend.height - 1  # 取最后一条
        else:
            index = self.get_index(id)

        if index is None or index < 0 or index > self.df_trend.height - 1:
            return None

        trend = Trend(**self.df_trend.row(index, named=True))
        # 有没有指定id
        if id:
            if is_completed is None:
                return trend
            else:
                return trend if trend.is_completed == is_completed else None
        else:
            # 没有指定id
            if is_completed is None:  # a. 对状态没有要求
                return trend
            else:
                if (
                    trend.is_completed == is_completed
                ):  # b. 要求的状态正好与最后一条吻合
                    return trend
                else:  # c. 要求的状态与最后一条不吻合，查找最新的一条满足条件的数据
                    df = (
                        self.df_trend.tail(Const.LOOKBACK_LIMIT)
                        .filter(pl.col("is_completed") == is_completed)
                        .tail(1)
                    )
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

    def get_swing_list(self, trend: Trend) -> List[Swing] | None:
        return self.swing_manager.get_swing_list(trend.swing_start_id, trend.swing_end_id)
