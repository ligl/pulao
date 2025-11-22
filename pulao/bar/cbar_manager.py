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
        #
        # 对k线进行包含合并处理
        # 处理完成的k线列表由4种特征的k线组成，即：上升K线组、下降K线组、顶分形和底分形
        #
        # 包含合并处理逻辑
        # last_cbar_df：最新的两条数据
        # 第1行（倒数第二行）：合并时定方向用的bar
        # 第2行（最新行）：与sbar做比较，判断是否需要合并
        last_cbar_df = self.df_cbar.tail(2)
        high_price = 0
        low_price = 0
        start_index = 0
        end_index = 0
        is_bar_containment = False  # 是否有包含关系
        if last_cbar_df.height == 2:  # 已有构造数据列表
            row_direction = last_cbar_df.row(0, named=True)
            row_compare = last_cbar_df.row(1, named=True)

            if row_compare["high_price"] > row_direction["high_price"]:  # 向上
                direction = SwingDirection.UP
            elif row_compare["low_price"] < row_direction["low_price"]:  # 向下
                direction = SwingDirection.DOWN
            else:
                # 不应该执行此处代码，如果执行，说明之前的数据有问题！！！
                raise AssertionError(
                    f"K线合并错误，出现了不应出现的情况 in {self.__class__.__name__}"
                )
            if (
                row_compare["high_price"] >= sbar.high_price
                and row_compare["low_price"] <= sbar.low_price
            ):  # 内包，即row_compare包含sbar
                is_bar_containment = True
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
                row_compare["high_price"] <= sbar.high_price
                and row_compare["low_price"] >= sbar.low_price
            ):  # 外包，即sbar包含row_compare
                is_bar_containment = True
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

        elif last_cbar_df.height == 1:  # sbar为第2根
            # 丢弃被包含的bar
            row_compare = last_cbar_df.row(0, named=True)
            if (
                row_compare["high_price"] >= sbar.high_price
                and row_compare["low_price"] <= sbar.low_price
            ):  # 内包，即row_compare包含sbar
                is_bar_containment = True
                high_price = row_compare["high_price"]
                low_price = row_compare["low_price"]
                start_index = row_compare["start_index"]
                end_index = sbar.index
            elif (
                row_compare["high_price"] <= sbar.high_price
                and row_compare["low_price"] >= sbar.low_price
            ):  # 外包，即sbar包含row_compare
                is_bar_containment = True
                high_price = sbar.high_price
                low_price = sbar.low_price
                start_index = row_compare["start_index"]
                end_index = sbar.index
            else:  # 没有包含关系
                pass
        else:  # 尚未构造数据，sbar为第1根
            row_compare = None
            pass

        if not is_bar_containment:  # 没有包含关系
            high_price = sbar.high_price
            low_price = sbar.low_price
            start_index = sbar.index
            end_index = sbar.index
        else:  # 有包含关系，
            # 1. 把row_compare删除
            self.df_cbar = self.df_cbar.filter(pl.col("index") != row_compare["index"])
        # 2. 增加cbar
        row = {
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
            pl.DataFrame(
                [[row[col] for col in self.df_cbar.columns]],
                schema=self.df_cbar.schema,
                orient="row",
            )
        )  # append row

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
        # endregion

        # region 1. 如果相邻顶底分形有重叠区域，先调整为次级别
        def _process_fractal_overlap():
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
            prev_row = None
            # 从新数据往旧数据遍历，判断前后两个波段点是否有重叠区域
            for i in range(df_fractals.height - 1, -1, -1):
                curr_row = df_fractals.row(i, named=True)
                if prev_row is None:
                    prev_row = curr_row
                    continue

                prev_fractal = self.get_fractal(prev_row["index"])
                curr_fractal = self.get_fractal(curr_row["index"])
                if not curr_fractal.overlap(prev_fractal):
                    continue
                # 有重叠，修改后者为次级别（前者更可能先被人眼认定）
                if prev_row["swing_point_type"] == curr_row["swing_point_type"]:
                    # 两者为同向分形，把次高/次低的调整为次级别
                    if curr_row["swing_point_type"] == SwingPointType.HIGH:
                        if curr_row["high_price"] > prev_row["high_price"]:
                            if prev_row["swing_point_level"] != SwingPointLevel.MINOR:
                                prev_row["swing_point_level"] = SwingPointLevel.MINOR
                                cbar_list.append(prev_row)
                        else:
                            if curr_row["swing_point_level"] != SwingPointLevel.MINOR:
                                curr_row["swing_point_level"] = SwingPointLevel.MINOR
                                cbar_list.append(curr_row)
                    else:  # SwingPointType.LOW
                        if curr_row["low_price"] < prev_row["low_price"]:
                            if prev_row["swing_point_level"] != SwingPointLevel.MINOR:
                                prev_row["swing_point_level"] = SwingPointLevel.MINOR
                                cbar_list.append(prev_row)
                        else:
                            if curr_row["swing_point_level"] != SwingPointLevel.MINOR:
                                curr_row["swing_point_level"] = SwingPointLevel.MINOR
                                cbar_list.append(curr_row)
                else:
                    # 两者非同向分形
                    if prev_row["swing_point_level"] != SwingPointLevel.MINOR:
                        prev_row["swing_point_level"] = SwingPointLevel.MINOR
                        cbar_list.append(prev_row)
                prev_row = curr_row
            if cbar_list:
                self.update_swing_point_level(cbar_list)

        # endregion

        # region 2. 如果由次级别组成的波段，形成了一段上涨一段下跌的结构，则顶点调整为本级别，反之亦复如是
        def _process_secondary_swing():
            """
            # 疑问：次级别会有上涨或下跌趋势吗？只能是区间震荡吧
            2. 以顶底分形为准，对连续次级别构建HH/HL/LL/LH波段结构，判断波段方向是上涨、下跌还是横盘区间，然后对分形进行级别划定
            2.1 如果由次级别组成的波段，形成了一段上涨一段下跌的结构，则把端点调整为本级别
            2.2 如果次级别组成的波段，形成了横盘区间，把区间的最低点（若未来价格离开区间向上）或最高点（若未来离开向下）调整为本级别
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
            changed_cbar_list = []  # 需要调整级别的顶底分形列表
            # 从新数据往旧数据遍历
            for i in range(df_fractals.height - 1, -1, -1):
                curr_row = df_fractals.row(i, named=True)
                if curr_row["swing_point_level"] == SwingPointLevel.MINOR:
                    # 次级别开始，记录连续次级别，并判断趋势
                    secondary_fractal_list.append(curr_row)

                    if curr_row["swing_point_type"] == SwingPointType.HIGH:
                        if secondary_high is None:
                            secondary_high = curr_row
                        elif secondary_high["high_price"] < curr_row["high_price"]:
                            secondary_high = curr_row
                    else:
                        if secondary_low is None:
                            secondary_low = curr_row
                        elif secondary_low["low_price"] > curr_row["low_price"]:
                            secondary_low = curr_row

                elif curr_row["swing_point_level"] == SwingPointLevel.MAJOR:
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
                                    prev_primary_fractal["swing_point_type"]
                                    == curr_row["swing_point_type"]
                                ):
                                    # 两个本级别同方向，需要次级别的波动高低点相连
                                    if (
                                        curr_row["swing_point_type"]
                                        == SwingPointType.HIGH
                                    ):
                                        # 本级别顶分形，需要找次级别的最小底分形相连
                                        secondary_low["swing_point_level"] = (
                                            SwingPointLevel.MAJOR
                                        )
                                        # 更新数据源
                                        changed_cbar_list.append(secondary_low)
                                    else:
                                        # 找次级别顶分形相连
                                        secondary_high["swing_point_level"] = (
                                            SwingPointLevel.MAJOR
                                        )
                                        # 更新数据源
                                        changed_cbar_list.append(secondary_high)
                                else:
                                    # 两个本级别反向，正好顶底相连，需要判断是否需要调整高低点边线
                                    if (
                                        curr_row["swing_point_type"]
                                        == SwingPointType.HIGH
                                    ):
                                        # 本级别顶分形，判断当前分形的高点是否比次级别最高分形大
                                        if (
                                            curr_row["high_price"]
                                            < secondary_high["high_price"]
                                        ):
                                            # 更改端点
                                            curr_row["swing_point_level"] = (
                                                SwingPointLevel.MINOR
                                            )
                                            secondary_high["swing_point_level"] = (
                                                SwingPointLevel.MAJOR
                                            )
                                            # 更新数据源
                                            changed_cbar_list.append(secondary_high)
                                            changed_cbar_list.append(curr_row)
                                    else:
                                        # 本级别底分形，判断当前分形的低点是否比次级别最低分形小
                                        if (
                                            curr_row["low_price"]
                                            > secondary_low["low_price"]
                                        ):
                                            # 更改端点
                                            curr_row["swing_point_level"] = (
                                                SwingPointLevel.MINOR
                                            )
                                            secondary_low["swing_point_level"] = (
                                                SwingPointLevel.MAJOR
                                            )
                                            # 更新数据源
                                            changed_cbar_list.append(secondary_low)
                                            changed_cbar_list.append(curr_row)
                    else:  # 没有次级别
                        pass

                    secondary_fractal_list.clear()

                    prev_primary_fractal = curr_row

            if changed_cbar_list:
                self.update_swing_point_level(changed_cbar_list)

        # endregion

        # region 3. 处理连续同级别同向分形，比如连续多个顶分形，调整最后一个为本级别，其他为次级别
        def _process_consecutive_same_fractals():
            """
            处理本级别连续同向分形，比如连续多个顶分形，调整最后一个为本级别，其他为次级别
            """
            # 取最新的一段数据进行处理，久远的数据级别已经固定，不用处理
            df_fractals = self.df_cbar.tail(
                Const.LOOKBACK_LIMIT
                if self.df_cbar.height > Const.LOOKBACK_LIMIT
                else self.df_cbar.height
            ).filter(
                (pl.col("swing_point_type") != SwingPointType.NONE)
                & (pl.col("swing_point_level") == SwingPointLevel.MAJOR)
            )

            prev_row = None
            changed_cbar_list = []  # 需要调整级别的顶底分形列表
            # 从新数据往旧数据遍历
            for i in range(df_fractals.height - 1, -1, -1):
                curr_row = df_fractals.row(i, named=True)
                if prev_row is None:
                    prev_row = curr_row
                    continue
                if curr_row["swing_point_type"] == prev_row["swing_point_type"]:
                    # 两分形同向，处理级别
                    if curr_row["swing_point_type"] == SwingPointType.HIGH:
                        if curr_row["high_price"] < prev_row["high_price"]:
                            pass
                    else:
                        pass
                else:
                    # 顶底/底顶相连，不用处理
                    pass

        def _validate():
            """
            验证处理结果是否正确，结果必须是本级别高低点交错出现
            """

        # endregion
        # 依次执行处理函数
        _process_fractal_overlap()
        # _process_secondary_swing()
        # _process_consecutive_same_fractals()
        _validate()

    def update_swing_point_level(self, cbar_list: List):
        """
        更新分形中对应bar的波段高低点级别
        """
        if not cbar_list:
            return
        expr_sbar = pl.col("swing_point_level")
        expr_cbar = pl.col("swing_point_level")
        for cbar in cbar_list:
            sbar_df = self.sbar_manager.get_range_index(
                cbar["start_index"],
                cbar["end_index"],
            )
            # 顶分形取最高价的bar，低分形取最低价的bar
            if cbar["swing_point_type"] == SwingPointType.LOW:
                bar_index = (
                    sbar_df.filter(pl.col("low_price") == pl.col("low_price").min())
                    .select("index")
                    .item()
                )
            elif cbar["swing_point_type"] == SwingPointType.HIGH:
                bar_index = (
                    sbar_df.filter(pl.col("high_price") == pl.col("high_price").max())
                    .select("index")
                    .item()
                )
            else:  # 不应该执行这里
                raise AssertionError("波段类型参数不对")

            expr_sbar = (
                pl.when(pl.col("index") == bar_index)
                .then(pl.lit(cbar["swing_point_level"]))
                .otherwise(expr_sbar)
            )
            expr_cbar = (
                pl.when(pl.col("index") == cbar["index"])
                .then(pl.lit(cbar["swing_point_level"]))
                .otherwise(expr_cbar)
            )
            # 1. 更新数据源-sbar_df
            self.sbar_manager.update(expr_sbar.alias("swing_point_level"))
            # 2. 更新数据源-cbar_df
            self.df_cbar = self.df_cbar.with_columns(
                expr_cbar.alias("swing_point_level")
            )

    def get_fractal(self, index: int)->Fractal | None:
        start_index = index - 1
        end_index = index + 1
        rows = self.df_cbar.slice(start_index, end_index - start_index + 1).rows(
            named=True
        )
        if len(rows) != 3:
            return None
        return Fractal(left=CBar(**rows[0]),middle=CBar(**rows[1]),right=CBar(**rows[2]))
