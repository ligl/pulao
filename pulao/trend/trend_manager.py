from __future__ import annotations
import copy
from typing import Any, List

import polars as pl
from datetime import datetime as Datetime

from pulao.events import Observable
from .trend import Trend
from pulao.bar import Fractal
from pulao.constant import (
    EventType,
    Direction,
    FractalType,
    Const,
    Timeframe,
)
from pulao.logging import get_logger
from pulao.swing import Swing, SwingManager
from pulao.utils import IDGenerator

logger = get_logger(__name__)


class _TrendSFSeq:
    def __init__(
        self, trend_manager: TrendManager, sfs: List[Swing] = None, trend: Trend = None
    ):
        self.trend_manager: TrendManager = trend_manager
        self.sfs: List[Swing] = sfs if sfs else []  # 特征序列
        self.trend: Trend = trend  # 趋势

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
            is_inclusive = (
                prev_sf.high_price >= tmp_swing.high_price
                and prev_sf.low_price <= tmp_swing.low_price
            ) or (
                prev_sf.high_price <= tmp_swing.high_price
                and prev_sf.low_price >= tmp_swing.low_price
            )
            if is_inclusive:
                # 有包含关系
                if self.trend.direction == Direction.UP:  # 向上，取高中高，低中高
                    tmp_swing.high_price = max(tmp_swing.high_price, prev_sf.high_price)
                    tmp_swing.low_price = max(tmp_swing.low_price, prev_sf.low_price)
                else:  # 向下，取高中低，低中低
                    tmp_swing.high_price = min(tmp_swing.high_price, prev_sf.high_price)
                    tmp_swing.low_price = min(tmp_swing.low_price, prev_sf.low_price)
                self.sfs.pop()
                # logger.debug("特征序列发现包含关系", trend=self.trend, sfs=self.sfs)
            else:
                break
        self.sfs.append(tmp_swing)  # 添加特征序列元素

    def update_trend(self, last_swing: Swing):
        if not self.trend:  # 新建
            self.trend = Trend()
            self.trend.direction = last_swing.direction
            self.trend.swing_start_id = last_swing.id
            self.trend.sbar_start_id = last_swing.sbar_start_id
            self.trend.is_completed = False
            self.trend.swing_end_id = last_swing.id
            self.trend.sbar_end_id = last_swing.sbar_end_id
            self.trend.high_price = last_swing.high_price
            self.trend.low_price = last_swing.low_price
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
            has_gap = (
                left_sf.high_price < mid_sf.low_price
                if fractal_type == FractalType.TOP
                else left_sf.low_price > mid_sf.high_price
            )

        return has_gap

    def clear(self):
        self.sfs = []
        self.trend = None

    def clean_rebuild(self, last_trend: Trend):
        self.clear()
        self.trend = last_trend
        swing_list = self.trend_manager.swing_manager.get_swing_list(
            last_trend.swing_start_id, last_trend.swing_end_id
        )
        for swing in swing_list or []:
            self.agg_swing(swing)

    def split_pullback_trend(self):
        """
        以波段极值点为标准，把self拆分成active trend/sfs 和 pullback trend/sfs
        :return:
        """
        # 首先要确定是否有pullback_trend ，条件：active_trend中的分形有缺口
        pullback_trend_sfs = _TrendSFSeq(self.trend_manager)
        has_gap = False
        for swing in self.sfs:
            if self.has_gap():
                has_gap = True
                break

        if not has_gap:
            return pullback_trend_sfs

        pullback_start_swing = self.trend_manager.swing_manager.get_limit_swing(
            start_id=self.trend.swing_start_id,
            end_id=self.trend.swing_end_id,
            arg="max" if self.trend.direction == Direction.UP else "min",
            direction=self.trend.direction.opposite,
        )

        # 以pullback_start_swing为起点，找与active_trend相反的趋势特征序列
        pullback_trend_sfs.trend = Trend(
            direction=self.trend.direction.opposite,
            swing_start_id=pullback_start_swing.id,
            swing_end_id=self.trend.swing_end_id,
            sbar_start_id=pullback_start_swing.sbar_start_id,
            sbar_end_id=self.trend.sbar_end_id,
            high_price=pullback_start_swing.high_price,
            low_price=pullback_start_swing.low_price,
            is_completed=False,
        )

        pullback_swing_list = self.trend_manager.swing_manager.get_swing_list(
            pullback_trend_sfs.trend.swing_start_id,
            pullback_trend_sfs.trend.swing_end_id,
        )
        for swing in pullback_swing_list or []:  # 特征序列初始化
            pullback_trend_sfs.agg_swing(swing)
            pullback_trend_sfs.trend.high_price = max(
                swing.high_price, pullback_trend_sfs.trend.high_price
            )
            pullback_trend_sfs.trend.low_price = min(
                swing.low_price, pullback_trend_sfs.trend.low_price
            )

        return pullback_trend_sfs


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
            "span": pl.UInt32,
            "volume": pl.Float32,
            "start_oi": pl.Float32,
            "end_oi": pl.Float32,
            "is_completed": pl.Boolean,  # 还未被确认的趋势，即正在进行中的趋势，在实时行情中尚未被确认
            "created_at": pl.Datetime("ms"),
        }
        self.df_trend: pl.DataFrame = pl.DataFrame(schema=schema)
        self.swing_manager: SwingManager = swing_manager
        self.swing_manager.subscribe(self._on_swing_changed, EventType.SWING_CHANGED)
        self.id_gen = IDGenerator(worker_id=3)
        self.active_trend_sfs = _TrendSFSeq(self)  # 趋势和趋势的特征序列
        self.pullback_trend_sfs = _TrendSFSeq(self)  # 反向趋势和特征序列
        self.backtrack_trend_id = None
        self.symbol = self.swing_manager.symbol
        self.timeframe = self.swing_manager.timeframe

    def _on_swing_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        # 1. 趋势检测识别
        backtrack_swing_id = payload.get("backtrack_id", None)
        if backtrack_swing_id is None:
            self._build_trend()
        else:
            self._clean_backtrack(backtrack_swing_id)
            self._backtrack_replay(backtrack_swing_id)
        # 2. 保存结果
        self.write_parquet()
        self.notify(timeframe, EventType.TREND_CHANGED, backtrack_id=self.backtrack_trend_id)

    def _clean_backtrack(self, backtrack_swing_id: int):
        # 1. 清理df_trend
        df = self.df_trend.filter(
            (pl.col("swing_start_id") <= backtrack_swing_id)
            & (backtrack_swing_id <= pl.col("swing_end_id"))
        )
        if df.is_empty():
            return

        del_trend = Trend(**df.row(0, named=True))
        del_trend_idx = self.get_index(del_trend.id)

        self.df_trend = self.df_trend.slice(
            0, del_trend_idx
        )  # 删除从第一个traceback_id出现时的数据

        # 取出删除之前最后处理的swing,填补tend信息
        end_swing = self.swing_manager.get_nearest_swing(backtrack_swing_id, -1)

        if end_swing:  # 如果end_swing为None，说明df_swing在traceback_id之前已没有数据，重新构建趋势
            # 不为None，修改del_trend并重新添加到df_trend
            if del_trend.swing_start_id == backtrack_swing_id:  # 在趋势起点
                del_trend.swing_start_id = end_swing.id
                del_trend.sbar_start_id = end_swing.sbar_start_id

            del_trend.swing_end_id = end_swing.id
            del_trend.sbar_end_id = end_swing.sbar_end_id
            del_trend.high_price = max(del_trend.high_price, end_swing.high_price)
            del_trend.low_price = min(del_trend.low_price, end_swing.low_price)
            del_trend.is_completed = False
            self._append_trend(del_trend)

        # 2. 清理并重新构建辅助对象
        last_trend = self.get_active_trend()
        if last_trend:
            self.active_trend_sfs.clean_rebuild(last_trend)
            self.pullback_trend_sfs = self.active_trend_sfs.split_pullback_trend()
        else:
            self.active_trend_sfs.clear()
            self.pullback_trend_sfs.clear()

        # 3. 记录df_trend的变化点
        self.backtrack_trend_id = del_trend.id

    def _backtrack_replay(self, backtrack_swing_id: int = None):
        """
        查找要从哪个swing开始回放处理，取值min(backtrack_id, trend.end_id)
        """
        # 2. 获取需要处理的swing[backtrack_id, last_id]，进行回放
        swing_list = self.swing_manager.get_nearest_swing(backtrack_swing_id)
        for swing in swing_list or []:
            self._build_trend(swing)

    def _build_trend(self, curr_swing: Swing = None):
        """
        构建趋势
        """
        # 情况1. 首次创建
        if self.df_trend.is_empty():
            # 第一次创建趋势，需要有3个连续有重叠的波段组成
            swing_list = self.swing_manager.get_last_swing(3, include_active=False)

            if not swing_list or len(swing_list) != 3:
                logger.debug("波段数量不足3个，跳过", swing_list=swing_list)
                return
            swing_start, swing_middle, swing_end = swing_list

            if not swing_start.overlap(swing_middle, swing_end):
                return

            # 有重叠
            # 检查波段方向是否符合要求：上下上或下上下
            if (
                not swing_start.direction
                == swing_middle.direction.opposite
                == swing_end.direction
            ):
                return

            # 检查波段的高低点是否符合要求
            if swing_start.direction == Direction.DOWN:
                # 下降趋势 高低降低，低点也降低
                if not (
                    swing_end.high_price < swing_start.high_price
                    and swing_end.low_price < swing_start.low_price
                ):
                    return
            else:  # 上升趋势 高点
                if not (
                    swing_end.high_price > swing_start.high_price
                    and swing_end.low_price > swing_start.low_price
                ):
                    return

            # 第一个波段的起点为趋势的起点，最后一个波段的终点为趋势临时的终点
            new_trend = Trend(
                direction=swing_start.direction,
                swing_start_id=swing_start.id,
                swing_end_id=swing_end.id,
                sbar_start_id=swing_start.sbar_start_id,
                sbar_end_id=swing_end.sbar_end_id,
                high_price=max(swing_start.high_price, swing_end.high_price),
                low_price=min(swing_start.low_price, swing_end.low_price),
                is_completed=False,
            )
            self._append_trend(new_trend)
            logger.debug(
                "首次创建趋势",
                new_trend=new_trend,
                swing_start=swing_start,
                swing_end=swing_end,
                swing_middle=swing_middle,
            )
            # init active_trend
            self.active_trend_sfs.clear()
            self.active_trend_sfs.trend = new_trend
            self.active_trend_sfs.agg_swing(swing_middle)
            # clear pullback trend
            self.pullback_trend_sfs.clear()
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

        # 最新需要处理的波段
        last_swing = (
            self.swing_manager.get_last_swing() if not curr_swing else curr_swing
        )

        # 如果正在走回调趋势段
        if self.pullback_trend_sfs.trend:
            self._build_pullback_trend(last_swing)
            return  # 一旦有缺口，下面的逻辑就不能走了，要么创新极值延续原趋势，要么终结原趋势

        self.active_trend_sfs.update_trend(last_swing)
        self.active_trend_sfs.agg_swing(last_swing)  # 特征序列包含合并处理
        self._update_active_trend(self.active_trend_sfs.trend)

        # 最后3个元素是否组成分形，第1和第2个元素之间是否有缺口
        fractal_type = self.active_trend_sfs.get_fractal_type()

        # 用特征序列判断趋势是否终结
        if fractal_type == FractalType.NONE:
            return
        # active trend特征序列出现分形
        if self.active_trend_sfs.has_gap():
            # 有缺口，记录回调趋势的起始点
            self.pullback_trend_sfs = self.active_trend_sfs.split_pullback_trend()
        else:
            # 形成趋势转折点
            # 1. 终结原趋势active trend
            self._confirmed_trend(self.active_trend_sfs.trend)

            # 2. 开始一个新趋势
            new_trend_start_swing = self.swing_manager.next_swing(
                self.active_trend_sfs.trend.swing_end_id
            )
            if not new_trend_start_swing:
                self.pullback_trend_sfs.clear()
                self.active_trend_sfs.clear()
                logger.debug(
                    "next_swing is None,不应该出现的情况，只有完成波段被重新破坏且是最后一根，正好还是趋势开始的时候出现",
                    start_swing=new_trend_start_swing,
                    new_trend_direction=self.active_trend_sfs.trend,
                )
                return

            new_trend = Trend(
                direction=self.active_trend_sfs.trend.direction.opposite,
                swing_start_id=new_trend_start_swing.id,
                swing_end_id=last_swing.id,
                sbar_start_id=new_trend_start_swing.sbar_start_id,
                sbar_end_id=last_swing.sbar_end_id,
                high_price=max(new_trend_start_swing.high_price, last_swing.high_price),
                low_price=min(new_trend_start_swing.low_price, last_swing.low_price),
                is_completed=False,
            )

            self._append_trend(new_trend)

            # 3. 重整缓存
            self.pullback_trend_sfs.clear()
            self.active_trend_sfs.clean_rebuild(new_trend)
            logger.debug("创建新趋势", new_trend=new_trend, last_swing=last_swing)

    def _build_pullback_trend(self, last_swing: Swing):
        # 1. 是否创前趋势新高
        is_new_limit = False  # 波段是否超越趋势极值点
        if (
            self.active_trend_sfs.trend.direction == Direction.DOWN
            and last_swing.low_price < self.active_trend_sfs.trend.low_price
        ):  # 下降趋势被创新低，原趋势active_trend延续
            # 更新
            is_new_limit = True
        elif (
            self.active_trend_sfs.trend.direction == Direction.UP
            and last_swing.high_price > self.active_trend_sfs.trend.high_price
        ):  # 上涨趋势被创新高，原趋势active_trend延续
            is_new_limit = True

        if is_new_limit:
            # 原趋势active_trend延续
            self.active_trend_sfs.update_trend(last_swing)
            self.active_trend_sfs.agg_swing(last_swing)
            self._update_active_trend(self.active_trend_sfs.trend)
            # 清理pullback trend/sfs
            self.pullback_trend_sfs.clear()
            return

        # 2. 特征序列包含合并处理
        self.pullback_trend_sfs.agg_swing(last_swing)
        # 3. 分形判断
        fractal_type = self.pullback_trend_sfs.get_fractal_type()
        if fractal_type == FractalType.NONE:
            self.pullback_trend_sfs.update_trend(last_swing)

            self.active_trend_sfs.update_trend(last_swing)
            self.active_trend_sfs.agg_swing(last_swing)
            self._update_active_trend(self.active_trend_sfs.trend)
            return
        if (
            self.pullback_trend_sfs.trend.direction == Direction.UP
            and fractal_type == FractalType.TOP
        ) or (
            self.pullback_trend_sfs.trend.direction == Direction.DOWN
            and fractal_type == FractalType.BOTTOM
        ):
            # 1). 有分形，确认active_trend结束
            self.active_trend_sfs.update_trend(last_swing)
            self._confirmed_trend(self.active_trend_sfs.trend)
            logger.debug(
                "有缺口，且pullback trend出分形，终结的前趋势",
                active_trend=self.active_trend_sfs.trend,
                sfs=self.active_trend_sfs.sfs,
            )

            # 2). 此时，对于回调趋势的特征序列分两种情况，有缺口或无缺口
            # 2.1 无缺口，pullback trend 也结束了
            # 2.2 有缺口，需要等待后续确认
            if self.pullback_trend_sfs.has_gap():
                # 需要重构构建 active trend/sfs 和 pullback trend/sfs

                self.active_trend_sfs.clear()
                self.active_trend_sfs.trend = self.pullback_trend_sfs.trend
                self.active_trend_sfs.sfs = self.pullback_trend_sfs.sfs

                # 把跳空后面的波段重新给pullback_trend
                self.pullback_trend_sfs = self.active_trend_sfs.split_pullback_trend()
                logger.debug(
                    "pullback trend出分形，且有缺口，拆分pullback_trend_sfs结构",
                    active_trend=self.active_trend_sfs.trend,
                    active_sfs=self.active_trend_sfs.sfs,
                    pullback_trend=self.pullback_trend_sfs.trend,
                    pullback_sfs=self.pullback_trend_sfs.sfs,
                )

            else:
                # 1. 没有缺口，完成pullback trend
                self._confirmed_trend(self.pullback_trend_sfs.trend)
                logger.debug(
                    "pullback trend出分形，且pullback trend自身也满足终结条件",
                    pullback_trend=self.pullback_trend_sfs.trend,
                    pullback_sfs=self.pullback_trend_sfs.sfs,
                )

                # 2. 开始一个新趋势
                # pullback trend sfs没有缺口，pullback trend也完成了新的一段趋势
                new_trend_start_swing = self.swing_manager.next_swing(
                    self.pullback_trend_sfs.trend.swing_end_id
                )
                new_trend = Trend(
                    direction=self.pullback_trend_sfs.trend.direction.opposite,
                    swing_start_id=new_trend_start_swing.id,
                    swing_end_id=last_swing.id,
                    sbar_start_id=new_trend_start_swing.sbar_start_id,
                    sbar_end_id=last_swing.sbar_end_id,
                    high_price=max(
                        new_trend_start_swing.high_price, last_swing.high_price
                    ),
                    low_price=min(
                        new_trend_start_swing.low_price, last_swing.low_price
                    ),
                    is_completed=False,
                )

                new_trend = self._append_trend(new_trend)

                self.pullback_trend_sfs.clear()
                self.active_trend_sfs.clean_rebuild(new_trend)

                logger.debug("pullback trend，开启新趋势", new_trend=new_trend)
        else:  # 有分形，但与要求不符
            self.active_trend_sfs.agg_swing(last_swing)
            self.active_trend_sfs.update_trend(last_swing)
            self._update_active_trend(self.active_trend_sfs.trend)

            self.pullback_trend_sfs.agg_swing(last_swing)
            self.pullback_trend_sfs.update_trend(last_swing)
            logger.debug("pullback trend中发现分形，但不符合条件")

    def _confirmed_trend(self, trend: Trend):
        """
        确认趋势完成，调整趋势起点和终点为趋势内波段的最高/最低点，如果需要再调整前一个趋势的终点
        :param trend:
        :return:
        """
        logger.debug("调整高点低前的趋势", origin_trend=trend)
        trend.is_completed = True
        # 有可能需要调整趋势的起始点，比如上涨趋势，期间还有波段低点比起始点还低，此时就要调整1）本趋势的起点，2）前一趋势的终止点
        start_swing = self.swing_manager.get_limit_swing(
            trend.swing_start_id,
            trend.swing_end_id,
            "max" if trend.direction == Direction.DOWN else "min",
            trend.direction,
        )
        if start_swing.id != trend.swing_start_id:  # 需要调整趋势的起点
            # 需要调整趋势的起点
            # 1). 调整当前趋势的起点
            trend.swing_start_id = start_swing.id
            trend.sbar_start_id = start_swing.sbar_start_id
            trend.high_price = max(start_swing.high_price, trend.high_price)
            trend.low_price = min(start_swing.low_price, trend.low_price)

            # 2). 调整前一趋势的终点
            # 前一趋势即active trend之前完成的最后一个趋势
            prev_trend = self.get_trend(is_completed=True)
            logger.debug(
                "需要调整当前趋势的起点，以及前一趋势的终点",
                active_trend=trend,
                prev_trend=prev_trend,
            )
            if prev_trend:
                prev_end_swing = self.swing_manager.prev_swing(
                    trend.swing_start_id
                )  # 前一个趋势的终点就是当前趋势起点的前一个swing
                if prev_trend.swing_end_id != prev_end_swing.id:
                    prev_trend.swing_end_id = prev_end_swing.id
                    prev_trend.sbar_end_id = prev_end_swing.sbar_end_id
                    prev_trend.high_price = max(
                        prev_end_swing.high_price, prev_trend.high_price
                    )
                    prev_trend.low_price = min(
                        prev_end_swing.low_price, prev_trend.low_price
                    )
                    self._del_trend(prev_trend.id)
                    self._append_trend(prev_trend)
                    logger.debug(
                        "趋势高低点有变化，调整上一个趋势的终点", prev_trend=prev_trend
                    )

        # 调整趋势结束点，以趋势内波段极值点为结束点
        end_swing = self.swing_manager.get_limit_swing(
            trend.swing_start_id,
            trend.swing_end_id,
            "min" if trend.direction == Direction.DOWN else "max",
            trend.direction,
        )
        trend.swing_end_id = end_swing.id
        trend.sbar_end_id = end_swing.sbar_end_id
        trend.high_price = max(end_swing.high_price, trend.high_price)
        trend.low_price = min(end_swing.low_price, trend.low_price)
        self._update_active_trend(trend)
        logger.debug("趋势终结", trend=trend)

    def _del_active_trend(self):
        active_trend = self.get_active_trend()
        if active_trend:
            self.df_trend = self.df_trend.slice(
                0, self.df_trend.height - 1
            )  # 删除未完成的趋势

    def _append_trend(self, trend: Trend) -> Trend:
        # 添加强度属性
        stat_rlt = self.swing_manager.cbar_manager.sbar_manager.stat(
            trend.sbar_start_id, trend.sbar_end_id
        )
        if stat_rlt:
            trend.span = stat_rlt.get("span", 0)
            trend.volume = stat_rlt.get("volume", 0)
            trend.start_oi = stat_rlt.get("start_oi", 0)
            trend.end_oi = stat_rlt.get("end_oi", 0)
        new_trend = {
            "id": self.id_gen.get_id(),
            "direction": trend.direction.value,
            "swing_start_id": trend.swing_start_id,
            "swing_end_id": trend.swing_end_id,
            "sbar_start_id": trend.sbar_start_id,
            "sbar_end_id": trend.sbar_end_id,
            "high_price": trend.high_price,
            "low_price": trend.low_price,
            "span": trend.span,
            "volume": trend.volume,
            "start_oi": trend.start_oi,
            "end_oi": trend.end_oi,
            "is_completed": trend.is_completed,
            "created_at": Datetime.now(),
        }

        self.df_trend = self.df_trend.vstack(
            pl.DataFrame([new_trend], schema=self.df_trend.schema)
        )
        return Trend(**new_trend)

    def _update_active_trend(self, trend: Trend):
        """
        更新逻辑：先删除旧数据，再添加新数据，每条数据的id都不同
        """
        # 先删除再添加
        self._del_active_trend()
        self._append_trend(trend)

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

    def _del_trend(self, start_id: int, end_id: int = None):
        """
        删除指定区间[start_id,end_id]的趋势，如果end_id没有指定，则删除到末尾
        :param start_id:
        :param end_id:
        :return:
        """
        if end_id is None:
            self.df_trend = self.df_trend.filter(~(pl.col("id") >= start_id))
        else:
            self.df_trend = self.df_trend.filter(
                ~((pl.col("id") >= start_id) & (pl.col("id") <= end_id))
            )

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
        return self.swing_manager.get_swing_list(
            trend.swing_start_id, trend.swing_end_id
        )

    def get_last_trend(
        self, count: int = None, include_active: bool = True
    ) -> None | Trend | List[Trend]:
        if count is None:
            count = 1

        if include_active:
            df = self.df_trend.tail(count)
        else:
            df = (
                self.df_trend.tail(Const.LOOKBACK_LIMIT)
                .filter(pl.col("is_completed") == True)
                .tail(count)
            )

        if df.is_empty():
            return None
        if count == 1:
            return Trend(**df.row(0, named=True))
        return [Trend(**row) for row in df.rows(named=True)]

    def write_parquet(self):
        # TODO 实时行情不能这么做，需要考虑性能影响
        self.df_trend.write_parquet(
            Const.PARQUET_PATH.format(
                symbol=self.symbol, filename=f"trend_{self.timeframe}"
            ),
            compression="zstd",
            compression_level=3,
            statistics=False,
            mkdir=True,
        )

    def read_parquet(self):
        self.df_trend = pl.read_parquet(
            Const.PARQUET_PATH.format(
                symbol=self.symbol, filename=f"trend_{self.timeframe}"
            )
        )
        return self.df_trend

    def pretty_worker_id(self):
        return self.df_trend.with_columns(
            [
                ((pl.col("id") // (2**12)) % (2**10)).alias("worker_id_trend"),
                ((pl.col("swing_start_id") // (2**12)) % (2**10)).alias(
                    "worker_id_swing_start"
                ),
                ((pl.col("swing_end_id") // (2**12)) % (2**10)).alias(
                    "worker_id_swing_end"
                ),
                ((pl.col("sbar_start_id") // (2**12)) % (2**10)).alias(
                    "worker_id_sbar_start"
                ),
                ((pl.col("sbar_end_id") // (2**12)) % (2**10)).alias(
                    "worker_id_sbar_end"
                ),
            ]
        )
