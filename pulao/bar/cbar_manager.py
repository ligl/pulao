from __future__ import annotations

from typing import List, Any

from pulao.constant import (
    SwingPointType,
    SwingPointLevel,
    Const,
    EventType,
    SwingDirection,
)
from pulao.events import Observable
from pulao.bar import SBar, SBarManager, CBar, Fractal

import polars as pl


class CBarManager(Observable):
    sbar_manager: SBarManager
    df_cbar: pl.DataFrame  # 包含合并后的k线列表

    def __init__(self, sbar_manager: SBarManager):
        super().__init__()
        schema = {
            "index": pl.UInt32,
            "start_index": pl.UInt32,
            "end_index": pl.UInt32,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "swing_point_type": pl.Utf8,  # 波段高低点标记
            "swing_point_level": pl.UInt8,  # 波段高低点级别（调整后的，正式用）
            "swing_point_level_origin": pl.UInt8,  # 波段高低点级别（原始级别）
        }
        self.df_cbar = pl.DataFrame(schema=schema)
        self.sbar_manager = sbar_manager
        self.sbar_manager.subscribe(self._on_sbar_created)

    def _on_sbar_created(self, event: EventType, payload: Any):
        self.detect(payload)

    def detect(self, sbar: SBar = None):
        # 波段检测识别
        # 1. K线包含处理
        self._agg_bar(sbar)
        # 2. 波段点识别
        self._detect_swing_point()

        self.notify(EventType.CBAR_CREATED)

    def _agg_bar(self, sbar: SBar):
        """
        K线包含关系处理（缠论预处理第一步）
        将原始K线合并为无包含关系的处理后K线序列
        """
        # 当前待处理的原始K线
        curr_high = sbar.high_price
        curr_low = sbar.low_price
        curr_idx = sbar.index

        # 用于构建新合并K线的临时变量
        merged_high = curr_high
        merged_low = curr_low
        start_idx = curr_idx
        end_idx = curr_idx

        # 情况1：df_cbar 为空 → 直接加入第一根
        if self.df_cbar.is_empty():
            self._append_cbar(start_idx, end_idx, merged_high, merged_low)
            return

        # 情况2：已有数据，取最后一根处理后的K线
        last_cbar_dict = self.df_cbar.tail(1).row(0, named=True)
        last_cbar = CBar(**last_cbar_dict)

        last_high = last_cbar.high_price
        last_low = last_cbar.low_price

        # 判断两根K线是否存在包含关系
        def is_inclusive(a_high, a_low, b_high, b_low):
            return (a_high >= b_high and a_low <= b_low) or (a_high <= b_high and a_low >= b_low)

        # 判断趋势方向（通过最后一根处理后K线与再往前一根比较）
        direction = None
        if self.df_cbar.height >= 2:
            prev_dict = self.df_cbar.tail(2).row(0, named=True)
            prev_cbar = CBar(**prev_dict)
            if last_high > prev_cbar.high_price:
                direction = SwingDirection.UP
            elif last_low < prev_cbar.low_price:
                direction = SwingDirection.DOWN
            # else: 第一个合并段，还没有明确方向，后面会处理

        # 如果没有明确方向（只有1根），则按“先高后低”或“先低后高”定方向（常见做法）
        if direction is None:
            if curr_high >= curr_low:  # 正常情况
                if last_high >= last_low:
                    # 都阳线或十字，按收盘或最高最低定，简单处理：谁高谁定向上
                    direction = SwingDirection.UP if curr_high >= last_high else SwingDirection.DOWN
                else:
                    direction = SwingDirection.UP
            else:
                direction = SwingDirection.DOWN

        # 开始包含处理
        included = is_inclusive(last_high, last_low, curr_high, curr_low)

        if included:
            # 有包含关系 → 合并，且按已有趋势方向处理高低点
            start_idx = last_cbar.start_index

            if direction == SwingDirection.UP:
                merged_high = max(last_high, curr_high)
                merged_low = max(last_low, curr_low)  # 向上趋势，低点取较高的
            else:  # DOWN
                merged_high = min(last_high, curr_high)  # 向下趋势，高点取较低的
                merged_low = min(last_low, curr_low)

            # 移除最后一条（因为要被合并替换）
            self.df_cbar = self.df_cbar.slice(0, self.df_cbar.height - 1)

            # 关键：可能还需要向前继续合并！（你原代码缺失这点）
            # 例如：1→2（包含）→3（又被1包含），必须一直向前吃
            while self.df_cbar.height >= 2:
                # 取新的最后两根
                new_last = CBar(**self.df_cbar.tail(1).row(0, named=True))
                prev = CBar(**self.df_cbar.tail(2).row(0, named=True))

                if direction == SwingDirection.UP:
                    if new_last.high_price <= prev.high_price:
                        break  # 已经破坏向上趋势，停止向前合并
                else:
                    if new_last.low_price >= prev.low_price:
                        break  # 破坏向下趋势

                # 检查新last是否还被之前的包含
                if is_inclusive(prev.high_price, prev.low_price, new_last.high_price,
                                new_last.low_price):
                    # 继续合并
                    start_idx = prev.start_index
                    if direction == SwingDirection.UP:
                        merged_high = max(merged_high, prev.high_price)
                        merged_low = max(merged_low, prev.low_price)
                    else:
                        merged_high = min(merged_high, prev.high_price)
                        merged_low = min(merged_low, prev.low_price)
                    # 删除倒数第二根（现在成了最后）
                    self.df_cbar = self.df_cbar.slice(0, self.df_cbar.height - 1)
                else:
                    break
        else:
            # 无包含关系 → 直接作为新K线加入
            merged_high = curr_high
            merged_low = curr_low
            start_idx = curr_idx

        # 最终追加合并后的K线
        self._append_cbar(start_idx, end_idx if not included else curr_idx, merged_high, merged_low)

    def _append_cbar(self, start_index: int, end_index: int, high_price: float, low_price: float):
        """封装追加逻辑，避免重复代码"""
        new_cbar = {
            "index": self.df_cbar.height,
            "start_index": start_index,
            "end_index": end_index,
            "high_price": high_price,
            "low_price": low_price,
            "swing_point_type": SwingPointType.NONE.value,
            "swing_point_level": SwingPointLevel.NONE.value,
            "swing_point_level_origin": SwingPointLevel.NONE.value,
        }
        self.df_cbar = self.df_cbar.vstack(
            pl.DataFrame([new_cbar], schema=self.df_cbar.schema)
        )

    def _detect_swing_point(self):
        # 波段点识别
        # region 0. 算法说明
        # 0. 分形：分形由3根相邻K线组成，有顶分形（中间高两边低）和底分形两种（中间低两边高），需要在包含合并处理过的k线中进行
        # 1. 波段点定义：每个分形即视为一个波段点，
        # 2. 波段点判别方法：接收到sbar后，对前一个bar进行顶底分形的判定
        # endregion

        # region 1. 分形判断
        # 取最近的3条bar，判断是否为分形
        last_bar_df = self.df_cbar.tail(3)
        if last_bar_df.height != 3:  # k线数量不够，不符合分形判断条数要求
            return

        left_bar = last_bar_df.row(0, named=True)
        middle_bar = last_bar_df.row(1, named=True)
        right_bar = last_bar_df.row(2, named=True)

        if (
            middle_bar["high_price"] > left_bar["high_price"]
            and middle_bar["high_price"] > right_bar["high_price"]
        ):  # 顶分形
            swing_point_type = SwingPointType.HIGH
        elif (
            middle_bar["low_price"] < left_bar["low_price"]
            and middle_bar["low_price"] < right_bar["low_price"]
        ):  # 底分形
            swing_point_type = SwingPointType.LOW
        else:  # 不是分形
            swing_point_type = SwingPointType.NONE
            pass
        # endregion

        if swing_point_type is not SwingPointType.NONE:  # 是分形
            swing_point_level_origin = SwingPointLevel.MAJOR  # 默认分形为本级别
            # region 2. 数据源更新
            # 是分形，更新分形标识和分形级别，更新cbar_df数据源、更新SBarManager
            self.df_cbar = self.df_cbar.with_columns(
                [
                    pl.when(pl.col("index") == middle_bar["index"])
                    .then(pl.lit(swing_point_type.value))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(pl.col("index") == middle_bar["index"])
                    .then(pl.lit(swing_point_level_origin.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
                    pl.when(pl.col("index") == middle_bar["index"])
                    .then(pl.lit(swing_point_level_origin.value))
                    .otherwise(pl.col("swing_point_level_origin"))
                    .alias("swing_point_level_origin"),
                ]
            )

            # 更新SBarManager数据源
            # 通过cbar中记录的属性，找到[start_index,end_index]最高/最低的那个值所在的k线，作为分形的 middle kbar
            sbar_df = self.sbar_manager.get_range_index(
                middle_bar["start_index"], middle_bar["end_index"]
            )

            # 获取原始（包含合并处理前）的bar在序列中的索引位置
            sbar_index = (
                sbar_df.filter(
                    pl.when(swing_point_type == SwingPointType.LOW)
                    .then(pl.col("low_price").min() == pl.col("low_price"))
                    .otherwise(pl.col("high_price").max() == pl.col("high_price"))
                )
                .select("index")
                .item()
            )
            self.sbar_manager.update(
                [
                    pl.when(pl.col("index") == sbar_index)
                    .then(pl.lit(swing_point_type.value))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(pl.col("index") == sbar_index)
                    .then(pl.lit(swing_point_level_origin.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
                    pl.when(pl.col("index") == sbar_index)
                    .then(pl.lit(swing_point_level_origin.value))
                    .otherwise(pl.col("swing_point_level_origin"))
                    .alias("swing_point_level_origin"),
                ]
            )
            # endregion

            self._adjust_swing_point_level()
        else:  # 不是分形
            pass

    def _adjust_swing_point_level(self):
        """
        调整波段点级别
        """

        # region 0. 算法说明
        #
        # 原则：人看着是就是
        # 目标：符合交易员在图表中人为划分的尺度
        # 级别：分两种，
        # 1. 本级别：当前图表明显的转折点
        # 2. 次级别：有分形，但波动不明显，交易员不会认为是本级别转折点
        # 级别调整规则：
        # 1. 如果相邻顶底分形有重叠区域，先调整为次级别
        # 2. 以顶底分形为准，对连续次级别构建HH/HL/LL/LH波段结构，判断波段方向是上涨、下跌还是横盘区间，然后对分形进行级别划定
        # 2.1 如果由次级别组成的波段，形成了一段上涨一段下跌的结构，则把端点调整为本级别
        # 2.2 如果次级别组成的波段，形成了横盘区间，把区间的最低点（若未来价格离开区间向上）或最高点（若未来离开向下）调整为本级别
        # 3. 处理连续同级别同向分形，
        # 3.1 如果有连续多个顶分形，调整最后一个为本级别，其他为次级别
        # 3.2 如果有连续多个底分形，调整最后一个为本级别，其他为次级别
        #
        # 步骤，
        # 1. 包含合并处理，顶底分形判断基础生成，默认分形都为本级别
        # 2. 分形重叠处理，把低波动噪音去掉，有重叠的分形降级为次级别
        # 3. 本级别连续同向分形处理，连续同向分形保留最后一个，其他降级为次级别
        # 4. 异常强劲的次级别处理，提升为本级别【具体怎么做？】
        #
        # endregion

        # 依次执行处理函数
        while True:
            self._process_fractal_overlap()
            self._process_consecutive_same_fractals()
            # self._process_secondary_swing()
            if self._validate():
                break

    def _process_fractal_overlap(self):
        """
        如果相邻顶底分形有重叠区域，先调整为次级别
        """
        # region 算法说明
        #
        # 1. 逐步计算相邻分形是否有重叠区域
        # 1.1 如果有重叠，检查是否有同向分形，
        # 1.2 如果为同向分形，把次高/次低的调整为次级别
        # 1.3 如果不是同向分形，把后者调整为次级别（前者更可能先被人眼认定）
        #
        # endregion

        # 取最新的一段数据进行重整，久远的数据级别已经固定，不用处理
        df_fractals = self.df_cbar.tail(
            Const.LOOKBACK_LIMIT
            if self.df_cbar.height > Const.LOOKBACK_LIMIT
            else self.df_cbar.height
        ).filter(pl.col("swing_point_type") != SwingPointType.NONE)

        cbar_list = []
        prev_cbar = None
        # 从新数据往旧数据遍历，判断前后两个波段点是否有重叠区域
        for i in range(df_fractals.height - 1, -1, -1):
            curr_cbar = CBar(**df_fractals.row(i, named=True))
            if prev_cbar is None:
                prev_cbar = curr_cbar
                continue

            prev_fractal = self.get_fractal(prev_cbar.index)
            curr_fractal = self.get_fractal(curr_cbar.index)
            if not curr_fractal.overlap(prev_fractal):
                continue
            # 有重叠，修改后者为次级别（前者更可能先被人眼认定）
            if prev_cbar.swing_point_type == curr_cbar.swing_point_type:
                # 两者为同向分形，把次高/次低的调整为次级别
                if curr_cbar.swing_point_type == SwingPointType.HIGH:
                    if curr_cbar.high_price > prev_cbar.high_price:
                        if prev_cbar.swing_point_level != SwingPointLevel.MINOR:
                            prev_cbar.swing_point_level = SwingPointLevel.MINOR
                            cbar_list.append(prev_cbar)
                    else:
                        if curr_cbar.swing_point_level != SwingPointLevel.MINOR:
                            curr_cbar.swing_point_level = SwingPointLevel.MINOR
                            cbar_list.append(curr_cbar)
                else:  # SwingPointType.LOW
                    if curr_cbar.low_price < prev_cbar.low_price:
                        if prev_cbar.swing_point_level != SwingPointLevel.MINOR:
                            prev_cbar.swing_point_level = SwingPointLevel.MINOR
                            cbar_list.append(prev_cbar)
                    else:
                        if curr_cbar.swing_point_level != SwingPointLevel.MINOR:
                            curr_cbar.swing_point_level = SwingPointLevel.MINOR
                            cbar_list.append(curr_cbar)
            else:
                # 两者非同向分形
                if prev_cbar.swing_point_level != SwingPointLevel.MINOR:
                    prev_cbar.swing_point_level = SwingPointLevel.MINOR
                    cbar_list.append(prev_cbar)
            prev_cbar = curr_cbar
        if cbar_list:
            self.update_swing_point_level(cbar_list)

    def _process_secondary_swing(self):
        """
        # 2. 以次级别顶底分形为准，对连续次级别构建HH/HL/LL/LH波段结构，判断波段方向是上涨、下跌还是横盘区间，然后对分形进行级别升降
        # 2.1 如果由次级别组成的波段，形成了一段上涨一段下跌的结构，则把端点调整为本级别
        # 2.2 如果次级别组成的波段，形成了横盘区间，把区间的最低点（若未来价格离开区间向上）或最高点（若未来离开向下）调整为本级别 [这里是不是不用考虑盘整区间了?]
        """
        # 取最新的一段数据进行处理，久远的数据级别已经固定，不用处理
        df_fractals = self.df_cbar.tail(
            Const.LOOKBACK_LIMIT
            if self.df_cbar.height > Const.LOOKBACK_LIMIT
            else self.df_cbar.height
        ).filter(pl.col("swing_point_type") != SwingPointType.NONE)

        secondary_fractal_list = []
        prev_primary_fractal = None
        secondary_high = None  # 一段连续次级别中最高的顶分形
        secondary_low = None  # 一段连续次级别中最低的底分形
        level_changed_cbar_list = []  # 需要调整级别的顶底分形列表
        # 从新数据往旧数据遍历
        for i in range(df_fractals.height - 1, -1, -1):
            curr_cbar = CBar(**df_fractals.row(i, named=True))
            if curr_cbar.swing_point_level == SwingPointLevel.MINOR:
                # 次级别开始，记录连续次级别，并判断波段区间的高低点
                secondary_fractal_list.append(curr_cbar)

                if curr_cbar.swing_point_type == SwingPointType.HIGH:
                    if secondary_high is None:
                        secondary_high = curr_cbar
                    elif secondary_high.high_price < curr_cbar.high_price:
                        secondary_high = curr_cbar
                else:
                    if secondary_low is None:
                        secondary_low = curr_cbar
                    elif secondary_low.low_price > curr_cbar.low_price:
                        secondary_low = curr_cbar

            elif curr_cbar.swing_point_level == SwingPointLevel.MAJOR:
                # 本级别开始
                # 判断次级别趋势，调整级别，清空次级别列表重新记录
                if secondary_high:  # 说明两个本级别之间存在次级别
                    if (
                        secondary_high is None or secondary_low is None
                    ):  # 说明两个本级别之间只有一个次级别
                        # 疑问：实盘中什么情况下会出现？好像不可能，如果两个本级别之间只有一个次级别，那次级别是怎么确认的？
                        pass
                    else:  # 有多个次级别
                        if prev_primary_fractal is None:
                            # 本级别尚未走完
                            pass
                        else:  # 次级别是在两个本级别中间
                            if (
                                prev_primary_fractal.swing_point_type
                                == curr_cbar.swing_point_type
                            ):
                                # 两个本级别同方向，需要次级别的波动高低点相连
                                if curr_cbar.swing_point_type == SwingPointType.HIGH:
                                    # 本级别顶分形，需要找次级别的最小底分形相连
                                    secondary_low.swing_point_level = (
                                        SwingPointLevel.MAJOR
                                    )
                                    # 更新数据源
                                    level_changed_cbar_list.append(secondary_low)
                                else:
                                    # 找次级别顶分形相连
                                    secondary_high.swing_point_level = (
                                        SwingPointLevel.MAJOR
                                    )
                                    # 更新数据源
                                    level_changed_cbar_list.append(secondary_high)
                            else:
                                # 两个本级别反向，正好顶底相连，需要判断是否需要调整高低点边线
                                if curr_cbar.swing_point_type == SwingPointType.HIGH:
                                    # 本级别顶分形，判断当前分形的高点是否比次级别最高分形大
                                    if curr_cbar.high_price < secondary_high.high_price:
                                        # 更改端点
                                        curr_cbar.swing_point_level = (
                                            SwingPointLevel.MINOR
                                        )
                                        secondary_high.swing_point_level = (
                                            SwingPointLevel.MAJOR
                                        )
                                        # 更新数据源
                                        level_changed_cbar_list.append(secondary_high)
                                        level_changed_cbar_list.append(curr_cbar)
                                else:
                                    # 本级别底分形，判断当前分形的低点是否比次级别最低分形小
                                    if curr_cbar.low_price > secondary_low.low_price:
                                        # 更改端点
                                        curr_cbar.swing_point_level = (
                                            SwingPointLevel.MINOR
                                        )
                                        secondary_low.swing_point_level = (
                                            SwingPointLevel.MAJOR
                                        )
                                        # 更新数据源
                                        level_changed_cbar_list.append(secondary_low)
                                        level_changed_cbar_list.append(curr_cbar)
                else:  # 没有次级别
                    pass

                secondary_fractal_list.clear()

                prev_primary_fractal = curr_cbar

        if level_changed_cbar_list:
            self.update_swing_point_level(level_changed_cbar_list)

    def _process_consecutive_same_fractals(self):
        """
        处理本级别连续同向分形，保留最近一个，其他降为次级别
        """

        def first():
            # 取最新的一段数据进行处理，久远的数据级别已经固定，不用处理
            df_major_fractals = self.df_cbar.tail(
                Const.LOOKBACK_LIMIT
                if self.df_cbar.height > Const.LOOKBACK_LIMIT
                else self.df_cbar.height
            ).filter(
                (pl.col("swing_point_type") != SwingPointType.NONE)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )

            prev_cbar = None
            changed_cbar_list = []  # 需要升降级的波段点列表
            # 从新数据往旧数据遍历
            for i in range(df_major_fractals.height - 1, -1, -1):
                curr_cbar = CBar(**df_major_fractals.row(i, named=True))
                if prev_cbar is None:
                    prev_cbar = curr_cbar
                    continue
                if curr_cbar.swing_point_type == prev_cbar.swing_point_type:
                    # 两分形同向，保留最新的一个，其他都降级
                    curr_cbar.swing_point_level = SwingPointLevel.MINOR
                    changed_cbar_list.append(curr_cbar)
                else:
                    # 顶底/底顶相连，不用处理
                    pass
                prev_cbar = curr_cbar

            self.update_swing_point_level(changed_cbar_list)

        def second():
            # 检查本级别顶底分形之间是否有越过其边界的次级别分形，如果有把那个次级别升级，对应分形的本级别降级
            df_major_fractals = self.df_cbar.tail(
                Const.LOOKBACK_LIMIT
                if self.df_cbar.height > Const.LOOKBACK_LIMIT
                else self.df_cbar.height
            ).filter(
                (pl.col("swing_point_type") != SwingPointType.NONE)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )
            prev_cbar = None
            level_changed_bars = []  # 需要降级的分形列表
            # 从新数据往旧数据遍历
            for i in range(df_major_fractals.height - 1, -1, -1):
                curr_cbar = CBar(**df_major_fractals.row(i, named=True))
                if prev_cbar is None:
                    prev_cbar = curr_cbar
                    continue
                if curr_cbar.swing_point_type != prev_cbar.swing_point_type:  # 一顶一底
                    # 找顶底分形中的次级别bar
                    tmp_df = self.df_cbar.slice(
                        curr_cbar.index, prev_cbar.index + 1
                    ).filter(
                        (pl.col("swing_point_type") != SwingPointType.NONE)
                        & (pl.col("swing_point_level") == SwingPointLevel.MINOR)
                    )
                    if curr_cbar.swing_point_type == SwingPointType.HIGH:
                        tmp_df = tmp_df.filter(
                            (pl.col("high_price") == pl.col("high_price").max())
                            & (pl.col("swing_point_type") == SwingPointType.HIGH)
                        )
                        if tmp_df.height > 0:  # 有次级别波段点
                            secondary_cbar = CBar(**tmp_df.row(0, named=True))
                            if curr_cbar.high_price < secondary_cbar.high_price:
                                secondary_cbar.swing_point_level = SwingPointLevel.MAJOR
                                level_changed_bars.append(secondary_cbar)
                                curr_cbar.swing_point_level = SwingPointLevel.MINOR
                                level_changed_bars.append(curr_cbar)
                    else:
                        tmp_df = tmp_df.filter(
                            (pl.col("low_price") == pl.col("low_price").min())
                            & (pl.col("swing_point_type") == SwingPointType.LOW)
                        )
                        if tmp_df.height > 0:  # 有次级别波段点
                            secondary_cbar = CBar(**tmp_df.row(0, named=True))
                            if curr_cbar.low_price > secondary_cbar.low_price:
                                secondary_cbar.swing_point_level = SwingPointLevel.MAJOR
                                level_changed_bars.append(secondary_cbar)
                                curr_cbar.swing_point_level = SwingPointLevel.MINOR
                                level_changed_bars.append(curr_cbar)

                prev_cbar = curr_cbar

            self.update_swing_point_level(level_changed_bars)

        # 执行
        first()
        second()

    def _validate(self):
        """
        验证处理结果是否正确，结果必须是本级别高低点交错出现
        """
        return True

    def update_swing_point_level(self, cbar_list: List[CBar]):
        """
        更新分形中对应bar的波段高低点级别
        """
        if not cbar_list:
            return
        expr_sbar = pl.col("swing_point_level")
        expr_cbar = pl.col("swing_point_level")
        for cbar in cbar_list:
            sbar_df = self.sbar_manager.get_range_index(
                cbar.start_index,
                cbar.end_index,
            )
            # 顶分形取最高价的bar，低分形取最低价的bar
            if cbar.swing_point_type == SwingPointType.LOW:
                bar_index = (
                    sbar_df.filter(pl.col("low_price") == pl.col("low_price").min())
                    .select("index")
                    .item()
                )
            elif cbar.swing_point_type == SwingPointType.HIGH:
                bar_index = (
                    sbar_df.filter(pl.col("high_price") == pl.col("high_price").max())
                    .select("index")
                    .item()
                )
            else:  # 不应该执行这里
                raise AssertionError("波段类型参数不对")

            expr_sbar = (
                pl.when(pl.col("index") == bar_index)
                .then(pl.lit(cbar.swing_point_level))
                .otherwise(expr_sbar)
            )
            expr_cbar = (
                pl.when(pl.col("index") == cbar.index)
                .then(pl.lit(cbar.swing_point_level))
                .otherwise(expr_cbar)
            )
            # 1. 更新数据源-sbar_df
            self.sbar_manager.update(expr_sbar.alias("swing_point_level"))
            # 2. 更新数据源-cbar_df
            self.df_cbar = self.df_cbar.with_columns(
                expr_cbar.alias("swing_point_level")
            )

    def get_fractal(self, index: int) -> Fractal | None:
        start_index = index - 1
        end_index = index + 1
        rows = self.df_cbar.slice(start_index, end_index - start_index + 1).rows(
            named=True
        )
        if len(rows) != 3:
            return None
        return Fractal(
            left=CBar(**rows[0]), middle=CBar(**rows[1]), right=CBar(**rows[2])
        )
