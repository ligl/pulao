import copy
from typing import Any, List, Optional

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

class _TrendSFSeq:
    def __init__(self, sfs: List[Swing]=None, trend: Trend=None):
        self.sfs:List[Swing] = sfs if sfs else []# 特征序列
        self.trend:Trend = trend # 趋势

    def __bool__(self):
        return True if self.trend and self.sfs else False

    def agg_swing(self, swing: Swing):
        # 处理特征序列包含关系
        if swing.direction == self.trend.direction:
            return

        tmp_swing = copy.deepcopy(swing)
        while True:  # 循环往前处理包含
            if not self.sfs:
                break
            prev_sf: Swing = self.sfs[-1]
            # 判断个波段是否存在包含关系
            is_inclusive = (prev_sf.high_price >= tmp_swing.high_price and prev_sf.low_price <= tmp_swing.low_price) or (
                prev_sf.high_price <= tmp_swing.high_price and prev_sf.low_price >= tmp_swing.low_price
            )
            if is_inclusive:
                # 有包含关系
                if self.trend.direction == Direction.UP:
                    tmp_swing.high_price = max(tmp_swing.high_price, prev_sf.high_price)
                    tmp_swing.low_price = max(tmp_swing.low_price, prev_sf.low_price)
                else:
                    tmp_swing.high_price = min(tmp_swing.high_price, prev_sf.high_price)
                    tmp_swing.low_price = min(tmp_swing.low_price, prev_sf.low_price)
                self.sfs.pop()
                logger.debug("特征序列发现包含关系", trend=self.trend,sfs=self.sfs)
            else:
                break
        self.sfs.append(tmp_swing)  # 添加特征序列元素

    def update_trend(self, last_swing: Swing):
        if not self.trend: # 新建
            self.trend = Trend()
            self.trend.direction = last_swing.direction
            self.trend.swing_start_id = last_swing.id
            self.trend.sbar_start_id = last_swing.sbar_start_id
            self.trend.is_completed = False
        # 更新
        self.trend.swing_end_id = last_swing.id
        self.trend.sbar_end_id = last_swing.sbar_end_id
        self.trend.high_price = max(last_swing.high_price, self.trend.high_price)
        self.trend.low_price = min(last_swing.low_price, self.trend.low_price)

    def get_fractal_type(self):
        # 最后3个元素是否组成分形，第1和第2个元素之间是否有缺口
        right_sf = self.sfs[-1] if len(self.sfs) >= 1 else None
        mid_sf = self.sfs[-2] if len(self.sfs) >= 2 else None
        left_sf = self.sfs[-3] if len(self.sfs) >= 3 else None

        fractal_type = Fractal.verify(left_sf, mid_sf, right_sf)
        return fractal_type

    def has_gap(self):
        # 组成的分形是否有缺口
        right_sf = self.sfs[-1] if len(self.sfs) >= 1 else None
        mid_sf = self.sfs[-2] if len(self.sfs) >= 2 else None
        left_sf = self.sfs[-3] if len(self.sfs) >= 3 else None

        fractal_type = Fractal.verify(left_sf, mid_sf, right_sf)
        has_gap = False
        if left_sf and mid_sf and right_sf:
            has_gap = left_sf.high_price < mid_sf.low_price if fractal_type == FractalType.TOP else left_sf.low_price > mid_sf.high_price

        return has_gap

    def clear(self):
        self.sfs = []
        self.trend = None

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
        self.trend_sfs = _TrendSFSeq() # 趋势和趋势的特征序列
        self.opposite_trend_sfs = _TrendSFSeq() # 反向趋势和特征序列


    def _on_swing_changed(self, event: EventType, payload: Any):
        # 趋势检测识别
        # logger.debug("_on_cbar_created", payload=payload)
        if not payload or payload["backtrack_id"] is None:
            self._build_trend()
        else:
            self._clean_reset(payload["backtrack_id"])
            self._backtrack_replay(payload["backtrack_id"])

    def _clean_reset(self, swing_backtrack_id: int):
        # 1. 清理df_trend
        df = self.df_trend.filter(
            (pl.col("swing_start_id") <= swing_backtrack_id) & (
                swing_backtrack_id <= pl.col("swing_end_id"))
        )
        if df.is_empty():
            return

        del_trend = Trend(**df.row(0, named=True))
        del_trend_idx = self.get_index(del_trend.id)

        self.df_trend = self.df_trend.slice(0, del_trend_idx)  # 删除从第一个traceback_id出现时的数据

        # 取出删除之前最后处理的swing,填补tend信息
        end_swing = self.swing_manager.get_nearest_swing(swing_backtrack_id, -1)

        if end_swing:  # 如果end_swing为None，说明df_swing在traceback_id之前已没有数据，重新构建趋势
            # 不为None，修改del_trend并重新添加到df_trend
            if del_trend.swing_start_id == swing_backtrack_id:  # 在波段起点
                del_trend.swing_start_id = end_swing.id
                del_trend.swing_start_id = end_swing.sbar_start_id

            del_trend.swing_end_id = end_swing.id
            del_trend.sbar_end_id = end_swing.sbar_end_id
            del_trend.high_price = max(del_trend.high_price, end_swing.high_price)
            del_trend.low_price = min(del_trend.low_price, end_swing.low_price)
            del_trend.is_completed = False
            self._append_trend(del_trend)

        self.backtrack_id = del_trend.id

    def _backtrack_replay(self, backtrack_id: int = None):
        """
        查找要从哪个swing开始回放处理，取值min(backtrack_id, trend.end_id)
        """
        # 2. 获取需要处理的swing[backtrack_id, last_id]，进行回放
        swing_list = self.swing_manager.get_nearest_swing(backtrack_id)
        for swing in swing_list or []:
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
                self.trend_sfs.clear()
                self.trend_sfs.trend = new_trend
                self.trend_sfs.agg_swing(swing_middle)
            return  # 首次尝试构建结构

        # 情况2. 已存在趋势，非首次构建
        # region 算法说明
        # 方法：特征序列判断趋势转折点
        # 1. 特征序列sfs(Structural Feature Sequence)
        # 1.1 上涨趋势由向下的波段组成特征序列，（下跌对称）
        # 1.2 上涨趋势只考察特征序列的顶分形，顶分形的顶就是趋势转折点（下跌对称）
        # 1.3 特征序列需要先按照K线合并的相同规则进行包含合并处理，形成标准特征序列（默认说特征序列指标准特征序列）
        # 2. 特征序列判断方法以有无缺口分为两种情况：
        # (以上涨为例)
        # 2.1 分形第1根与第2根没有缺口
        # 2.1.1 那么分形的顶点就是转折点
        # 2.2 有缺口，那么需要以最高点为起点向下，建立新的特征序列，
        # 2.2.1 如果后续出现底分形，则最高点为趋势的转折点
        # 2.2.2 如果后续在价格创新高之前都没有出现底分形，则延续原有趋势
        #
        # endregion
        active_trend = self.get_active_trend()
        last_swing = self.swing_manager.get_last_swing()

        # 上一个趋势结束时，正好是所有swing用完之时，这里需要以当前last_swing新建趋势
        if active_trend is None:
            new_trend = Trend(
                direction=last_swing.direction,
                swing_start_id=last_swing.id,
                swing_end_id=last_swing.id,
                sbar_start_id=last_swing.sbar_start_id,
                sbar_end_id=last_swing.sbar_end_id,
                high_price=last_swing.high_price,
                low_price=last_swing.low_price,
                is_completed=False,
            )
            self._append_trend(new_trend)
            logger.debug("创建趋势[active_trend is None]", new_trend=new_trend)
            return

        if self.opposite_trend_sfs:
            # 1. 是否创前趋势新高
            is_new_limit = False # 波段是否超越趋势极值点
            if active_trend.direction == Direction.DOWN and last_swing.low_price < active_trend.low_price: # 下降趋势被创新低，原趋势active_trend延续
                    # 更新
                    is_new_limit = True
            elif active_trend.direction == Direction.UP and last_swing.high_price > active_trend.high_price: # 上涨趋势被创新高，原趋势active_trend延续
                    is_new_limit = True

            if is_new_limit:
                # 原趋势active_trend延续
                self.trend_sfs.update_trend(last_swing)
                self._update_active_trend(self.trend_sfs.trend)
                # 清理opposite trend/sfs
                self.opposite_trend_sfs.clear()
                return

            # 2. 特征序列包含合并处理
            self.opposite_trend_sfs.agg_swing(last_swing)
            # 3. 分形判断
            fractal_type = self.opposite_trend_sfs.get_fractal_type()
            if fractal_type == FractalType.NONE:
                self.opposite_trend_sfs.update_trend(last_swing)

                self.trend_sfs.update_trend(last_swing)
                self._update_active_trend(self.trend_sfs.trend)
                return
            if ((self.opposite_trend_sfs.trend.direction == Direction.UP and fractal_type == FractalType.TOP)
                or (self.opposite_trend_sfs.trend.direction == Direction.DOWN and fractal_type == FractalType.BOTTOM)):
                # 1. 在前高点终结原趋势
                end_swing = self.swing_manager.get_limit_swing(
                    start_id=active_trend.swing_start_id,
                    end_id=active_trend.swing_end_id,
                    arg="max" if active_trend.direction == Direction.UP else "min",
                    direction=active_trend.direction,
                )
                # 以end_swing为终点，结束原趋势
                self._update_active_trend(active_trend)
                self.trend_sfs.clear()

                # 2. 此时，原趋势反向趋势opposite_trend也终结了
                opposite_trend_start_swing = self.swing_manager.get_swing(self.opposite_trend_sfs.trend.swing_start_id)
                opposite_trend_end_swing = self.swing_manager.get_limit_swing(
                    start_id=opposite_trend_start_swing.id,
                    end_id=last_swing.id,
                    arg="max" if self.opposite_trend_sfs.trend.direction == Direction.UP else "min",
                    direction=self.opposite_trend_sfs.trend.direction,
                )
                opposite_trend = Trend(
                    direction=active_trend.opposite_direction,
                    swing_start_id=opposite_trend_start_swing.id,
                    swing_end_id=opposite_trend_start_swing.id,
                    sbar_start_id=opposite_trend_start_swing.sbar_start_id,
                    sbar_end_id=opposite_trend_end_swing.sbar_end_id,
                    high_price=max(opposite_trend_start_swing.high_price, opposite_trend_end_swing.high_price),
                    low_price=min(opposite_trend_start_swing.low_price, opposite_trend_end_swing.low_price),
                    is_completed=True,
                )
                self._append_trend(opposite_trend)
                self.opposite_trend_sfs.clear()

                # 3. 以opposite_trend终止点为起点创建新的active trend
                new_active_trend_start_swing = self.swing_manager.get_limit_swing(
                    start_id=opposite_trend.swing_end_id,
                    end_id=last_swing.id,
                    arg="max" if opposite_trend.direction == Direction.UP else "min",
                    direction=opposite_trend.opposite_direction,
                )
                new_active_trend = Trend(
                    direction=opposite_trend.opposite_direction,
                    swing_start_id=new_active_trend_start_swing.id,
                    swing_end_id=last_swing.id,
                    sbar_start_id=new_active_trend_start_swing.sbar_start_id,
                    sbar_end_id=last_swing.sbar_end_id,
                    high_price=max(new_active_trend_start_swing.high_price,
                                   last_swing.high_price),
                    low_price=min(new_active_trend_start_swing.low_price,
                                  last_swing.low_price),
                    is_completed=False,
                )
                self._append_trend(new_active_trend)
                return
            return # 一旦有缺口，下面的逻辑就不能走了，要么创新极值延续原趋势，要么终结原趋势


        self.trend_sfs.trend = active_trend
        self.trend_sfs.agg_swing(last_swing)# 特征序列包含合并处理

        # 最后3个元素是否组成分形，第1和第2个元素之间是否有缺口
        fractal_type = self.trend_sfs.get_fractal_type()

        # 用特征序列判断趋势是否终结
        if fractal_type == FractalType.NONE:
            self._update_active_trend(active_trend)
            return

        if self.trend_sfs.has_gap():
            # 有缺口
            # 2.2 有缺口，那么需要以最高点为起点向下，建立新的特征序列，
            # 2.2.1 如果后续出现底分形，则最高点为趋势的转折点
            # 2.2.2 如果后续在价格创新高之前都没有出现底分形，则延续原有趋势
            start_swing = self.swing_manager.get_limit_swing(
                start_id=active_trend.swing_start_id,
                end_id=active_trend.swing_end_id,
                arg="max" if active_trend.direction == Direction.UP else "min",
                direction=active_trend.opposite_direction,
            )
            # 以limit_swing为起点，找与active_trend相反的趋势特征序列
            opposite_trend = Trend(
                direction=active_trend.opposite_direction,
                swing_start_id=start_swing.id,
                swing_end_id=last_swing.id,
                sbar_start_id=start_swing.sbar_start_id,
                sbar_end_id=last_swing.sbar_end_id,
                high_price=max(start_swing.high_price,last_swing.high_price),
                low_price=min(start_swing.low_price,last_swing.low_price),
                is_completed=False,
            )
            self.opposite_trend_sfs.trend = opposite_trend
            self.opposite_trend_sfs.agg_swing(start_swing)
        else:
            # 形成趋势转折点
            active_trend.swing_end_id = last_swing.id
            active_trend.high_price = max(last_swing.high_price, active_trend.high_price)
            active_trend.low_price = min(last_swing.low_price, active_trend.low_price)
            active_trend.sbar_end_id = last_swing.sbar_end_id
            active_trend.is_completed = True

            active_trend = self._normal_trend(active_trend)
            self._update_active_trend(active_trend)
            logger.debug("趋势终结", active_trend=active_trend)

            start_swing = self.swing_manager.next_swing(active_trend.swing_end_id)
            if start_swing is None:
                logger.warning("已经没有swing了", current_swing=last_swing)
                return
            new_trend = Trend(
                direction=active_trend.opposite_direction,
                swing_start_id=start_swing.id,
                swing_end_id=last_swing.id,
                sbar_start_id=start_swing.sbar_start_id,
                sbar_end_id=last_swing.sbar_end_id,
                high_price=active_trend.high_price if active_trend.direction == Direction.UP else last_swing.high_price,
                low_price=last_swing.low_price if active_trend.direction == Direction.UP else active_trend.low_price,
                is_completed=False,
            )

            self._append_trend(new_trend)
            logger.debug("创建新趋势", new_trend=new_trend,
                         last_swing=last_swing)

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

    def get_index(self, id: int) -> int | None:
        idx = self.df_trend.select(pl.col("id").search_sorted(id)).item()
        if idx >= self.df_trend.height or self.df_trend["id"][idx] != id:
            return None
        else:
            return idx

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
