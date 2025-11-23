from typing import Any, List
from pulao.events import Observable
import polars as pl

from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPointType, SwingPointLevel, Const
from ..bar import CBarManager, CBar, Fractal


class SwingManager(Observable):
    cbar_manager: CBarManager
    df_swing : pl.DataFrame

    def __init__(self, cbar_manager: CBarManager):
        super().__init__()
        self.cbar_manager = cbar_manager
        self.cbar_manager.subscribe(self._on_cbar_created)
        schema = {
            "index": pl.UInt32,
            "start_index": pl.UInt32,
            "end_index": pl.UInt32,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "direction": pl.UInt8,
            "is_completed": pl.Boolean,
        }
        self.df_swing = pl.DataFrame(schema=schema)

    def _on_cbar_created(self, event: EventType, payload: Any):
        if event == EventType.FRACTAL_CONFIRMED:
            self.detect()

    def detect(self):
        # 波段检测识别
        self._adjust_swing_point_level()

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

            expr_cbar = (
                pl.when(pl.col("index") == cbar.index)
                .then(pl.lit(cbar.swing_point_level))
                .otherwise(expr_cbar)
            )
            # 更新数据源-cbar_df
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
