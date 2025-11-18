from typing import Any, Tuple

from pulao.events import Observable
import polars as pl

from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPointType, SwingPointLevel
from ..sbar import SBarManager, SBar


class _CBar:
    start_index: int  # 合并k线的开始索引（CBarManager）
    end_index: int  # 合并k线的结束索引
    high_price: float  # 合并后的最高价
    low_price: float  # 合并后的最低价
    swing_point: SwingPointType  # 波段高低点标识
    swing_point_level: SwingPointLevel  # 波段高低点级别


class SwingManager(Observable):
    sbar_manager: SBarManager()
    df_cbar: pl.DataFrame  # 包含合并后的k线列表

    def __init__(self, sbar_manager: SBarManager):
        super().__init__()
        schema = {
            "start_index": int,
            "end_index": int,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "swing_point_type": pl.Utf8,  # 波段高低点标记
            "swing_point_level": int,  # 波段高低点级别
        }
        self.df_cbar = pl.DataFrame(schema=schema)
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
        self._detect_swing_point()
        # 3. 给SBar标注
        self._mark_swing_point(sbar)
        # self.notify(EventType.SWING_CHANGED, self)

    def _agg_bar(self, sbar: SBar):
        #
        # 处理完成的k线列表由4种特征的k线组成，即：上升K线组、下降K线组、顶分形和底分形
        #

        # 对传入sbar做K线包含处理
        index = self.df_cbar.height - 1
        cbar_df = self.df_cbar.slice(index, 2)
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
        if high_price == 0 and low_price == 0:  # 没有包含关系
            high_price = sbar.high_price
            low_price = sbar.low_price
            start_index = sbar.index
            end_index = sbar.index
        else:  # 有包含关系，
            # 1. 把row_compare删除
            self.df_cbar = self.df_cbar.filter(
                pl.arange(0, self.df_cbar.height) != index
            )
        # 2. 增加sbar
        row = {
            "start_index": start_index,
            "end_index": end_index,
            "high_price": high_price,
            "low_price": low_price,
            "swing_point_type": "",
            "swing_point_level": 0,
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
        # region 算法说明
        # 0. 分形：分形由3根相邻K线组成，有顶分形（中间高两边低）和底分形两种（中间低两边高）
        # 1. 波段点定义：每个分形即视为一个波段点，
        # 2. 波段点级别：如果顶底分形有重叠区域，即视为次级波段点，否则视为本级波段点
        # 3. 波段点判别方法：接收到sbar后，对前一个bar进行顶底分形的判定
        # 判定分形级别
        # 1. 检查与临近的反向分形是否有重叠，
        # 1.1 如果重叠，则设置当前分形的级别为次级别
        # 1.2 如果没有重叠，检查反向分形与临近的同向分形是否有重叠
        # 1.2.1 如果有重叠，则更新临近的同向分形为次级别
        #
        #           |
        #          | |   临近的反向分形
        #             |   |
        #              | | |
        #               |   |
        #     临近的同向分形    |  |
        #                      |
        #                  当前分形
        #
        # endregion

        # region 分形判断
        # 取最近的3条bar，判断是否为分形
        last_bar_df = self.df_cbar.with_row_index("__index__").tail(3)
        if last_bar_df.height != 3:  # k线数量不够，不符合分形判断条数要求
            return

        left_bar = last_bar_df.row(0, named=True)
        middle_bar = last_bar_df.row(1, named=True)
        right_bar = last_bar_df.row(2, named=True)

        is_top_fractal = False
        is_bottom_fractal = False

        if (
            middle_bar["high_price"] > left_bar["high_price"]
            and middle_bar["high_price"] > right_bar["high_price"]
        ):  # 顶分形
            is_top_fractal = True
        elif (
            middle_bar["low_price"] < left_bar["low_price"]
            and middle_bar["low_price"] < right_bar["low_price"]
        ):  # 底分形
            is_bottom_fractal = True
        else:  # 不是分形
            pass
        # endregion

        if is_bottom_fractal or is_top_fractal:  # 是分形
            # region 找临近的底分形
            start_index = (
                100 if self.df_cbar.height > 100 else 0
            )  # 优化：不用遍历所有数据，取最近100条数据做为标准，试想：100条都没有重叠的数据，还想啥！！

            tmp_df = self.df_cbar.with_row_index("__index__").slice(
                start_index, self.df_cbar.height - 1
            )

            prev_bottom_tmp = tmp_df.filter(
                pl.col("swing_point_type") == SwingPointType.LOW.value
            ).tail(1)
            prev_top_tmp = tmp_df.filter(
                pl.col("swing_point_type") == SwingPointType.HIGH.value
            ).tail(1)

            prev_bottom_index = (
                prev_bottom_tmp.row(0, named=True)["__index__"]
                if not prev_bottom_tmp.is_empty()
                else 0
            )
            prev_top_index = (
                prev_top_tmp.row(0, named=True)["__index__"]
                if not prev_top_tmp.is_empty()
                else 0
            )

            prev_bottom = (
                self.df_cbar.slice(prev_bottom_index - 1, 3)
                if prev_bottom_index > 0
                else None
            )
            prev_top = (
                self.df_cbar.slice(prev_top_index - 1, 3)
                if prev_top_index > 0
                else None
            )

            # 前一个低分形
            prev_bottom_left = (
                prev_bottom.row(0, named=True)
                if prev_bottom is not None and not prev_bottom.is_empty()
                else None
            )
            prev_bottom_middle = (
                prev_bottom.row(1, named=True)
                if prev_bottom is not None and not prev_bottom.is_empty()
                else None
            )
            prev_bottom_right = (
                prev_bottom.row(2, named=True)
                if prev_bottom is not None and not prev_bottom.is_empty()
                else None
            )

            # 前一个顶分形
            prev_top_left = (
                prev_top.row(0, named=True)
                if prev_top is not None and not prev_top.is_empty()
                else None
            )
            prev_top_middle = (
                prev_top.row(1, named=True)
                if prev_top is not None and not prev_top.is_empty()
                else None
            )
            prev_top_right = (
                prev_top.row(2, named=True)
                if prev_top is not None and not prev_top.is_empty()
                else None
            )

            # endregion

            # 分形区间的最高价、最低价
            fractal_high_price, fractal_low_price = _get_fractal_range(
                left_bar, middle_bar, right_bar
            )
            prev_bottom_high_price, prev_bottom_low_price = (
                _get_fractal_range(
                    prev_bottom_left, prev_bottom_middle, prev_bottom_right
                )
                if prev_bottom_left is not None
                and prev_bottom_middle is not None
                and prev_bottom_right is not None
                else (None, None)
            )
            prev_top_high_price, prev_top_low_price = (
                _get_fractal_range(prev_top_left, prev_top_middle, prev_top_right)
                if prev_top_left is not None
                and prev_top_middle is not None
                and prev_top_right is not None
                else (None, None)
            )

            swing_point_type = SwingPointType.NONE
            swing_point_level = SwingPointLevel.CURRENT_TIMEFRAME  # 默认分形为本级别

            if is_top_fractal:  # 顶分形
                swing_point_type = SwingPointType.HIGH

                # 当前分形是否与前一个反向分形有重叠
                if _is_price_range_overlap(
                    prev_bottom_high_price,
                    prev_bottom_low_price,
                    fractal_high_price,
                    fractal_low_price,
                ):
                    # 有重叠，设置当前分形的级别为次级别
                    swing_point_level = SwingPointLevel.LOWER_TIMEFRAME
                else:
                    # 没有重叠，检查反向分形与临近的同向分形是否有重叠
                    if _is_price_range_overlap(
                        prev_bottom_high_price,
                        prev_bottom_low_price,
                        prev_top_high_price,
                        prev_top_low_price,
                    ):
                        # 有重叠，更新临近的同向分形为次级别
                        prev_top_level = SwingPointLevel.LOWER_TIMEFRAME
                        # 更新数据源
                        sbar_df = self.sbar_manager.get_range_index(
                            prev_bottom_middle["start_index"],
                            prev_bottom_middle["end_index"],
                            True,
                        )
                        prev_top_index = (
                            sbar_df.filter(pl.col("high_price") == pl.col("high_price").max())
                            .select("__index__")
                            .item()
                        )

                        self.sbar_manager.update_by_index(
                            prev_top_index, "swing_point_level", prev_top_level
                        )
            if is_bottom_fractal:  # 底分形
                swing_point_type = SwingPointType.LOW

                # 当前分形是否与前一个反向分形有重叠
                if _is_price_range_overlap(
                    prev_top_high_price,
                    prev_top_low_price,
                    fractal_high_price,
                    fractal_low_price,
                ):
                    # 有重叠，设置当前分形的级别为次级别
                    swing_point_level = SwingPointLevel.LOWER_TIMEFRAME
                else:
                    # 没有重叠，检查反向分形与临近的同向分形是否有重叠
                    if _is_price_range_overlap(
                        prev_bottom_high_price,
                        prev_bottom_low_price,
                        prev_top_high_price,
                        prev_top_low_price,
                    ):
                        # 有重叠，更新临近的同向分形为次级别
                        prev_bottom_level = SwingPointLevel.LOWER_TIMEFRAME
                        # 更新数据源
                        sbar_df = self.sbar_manager.get_range_index(
                            prev_bottom_middle["start_index"],
                            prev_bottom_middle["end_index"],
                            True,
                        )
                        prev_bottom_index = (
                            sbar_df.filter(pl.col("low_price") == pl.col("low_price").min())
                            .select("__index__")
                            .item()
                        )

                        self.sbar_manager.update_by_index(
                            prev_bottom_index, "swing_point_level", prev_bottom_level
                        )

            # region 数据源更新
            # 是分形，更新分形标识和分形级别，更新cbar_df数据源、更新SBarManager
            self.df_cbar = self.df_cbar.with_columns(
                [
                    pl.when(
                        pl.arange(0, self.df_cbar.height) == middle_bar["__index__"]
                    )
                    .then(pl.lit(swing_point_type))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(
                        pl.arange(0, self.df_cbar.height) == middle_bar["__index__"]
                    )
                    .then(pl.lit(swing_point_level.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
                ]
            )

            # 更新SBarManager数据源
            # 通过cbar中记录的属性，找到[start_index,end_index]最高/最低的那个值所在的k线，作为分形的 middle kbar
            sbar_df = self.sbar_manager.get_range_index(
                middle_bar["start_index"],
                middle_bar["end_index"],
                True,
            )

            # 获取原始（包含合并处理前）的bar在序列中的索引位置
            sbar_index = (
                sbar_df.filter(
                    pl.when(is_bottom_fractal)
                    .then(pl.col("low_price").min() == pl.col("low_price"))
                    .otherwise(pl.col("high_price").max() == pl.col("high_price"))
                )
                .select("__index__")
                .item()
            )
            self.sbar_manager.update(
                [
                    pl.when(pl.arange(0, self.sbar_manager.total_count) == sbar_index)
                    .then(pl.lit(swing_point_type))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(pl.arange(0, self.sbar_manager.total_count) == sbar_index)
                    .then(pl.lit(swing_point_level.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
                ]
            )
            # endregion
        else:  # 不是分形
            pass

    def _mark_swing_point(self, sbar: SBar):
        # 给SBar标注
        pass


def _get_fractal_range(left, middle, right) -> Tuple[float, float]:
    """
    获取分形区间
    :param left:
    :param middle:
    :param right:
    :return: (max,min)
    """
    high = max(
        float(left["high_price"]),
        float(right["high_price"]),
        float(middle["high_price"]),
    )
    low = min(
        float(left["low_price"]), float(right["low_price"]), float(middle["low_price"])
    )
    return high, low


def _is_price_range_overlap(
    a_high_price: float, a_low_price: float, b_high_price: float, b_low_price: float
) -> bool:
    """
    检查a与b的价格区间是否有重叠
    """
    if (
        a_high_price is None
        or a_low_price is None
        or b_high_price is None
        or b_low_price is None
    ):
        return False
    if a_low_price > b_high_price:
        return False
    if a_high_price < b_low_price:
        return False
    return True
