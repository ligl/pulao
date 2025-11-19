from typing import Any, Tuple

from polars import DataFrame

from pulao.events import Observable
import polars as pl

from pulao.swing import Swing
from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPointType, SwingPointLevel
from ..sbar import SBarManager, SBar


class SwingManager(Observable):
    sbar_manager: SBarManager()
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
            "swing_point_level": pl.UInt8,  # 波段高低点级别
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

        # self.notify(EventType.SWING_CHANGED, self)

    def _agg_bar(self, sbar: SBar):
        #
        # 处理完成的k线列表由4种特征的k线组成，即：上升K线组、下降K线组、顶分形和底分形
        #

        # 对传入sbar做K线包含处理
        last_index = self.df_cbar.height - 1
        cbar_df = self.df_cbar.slice(last_index, 2)
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
            self.df_cbar = self.df_cbar.filter(pl.col("index") != last_index)
        # 2. 增加sbar
        row = {
            "index": self.df_cbar.height,
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
        last_bar_df = self.df_cbar.tail(3)
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
            tmp_df = self.df_cbar.slice(
                0
            )  # 数据量大的时候可以考虑做优化，比如只找最近的100条

            prev_bottom_tmp = tmp_df.filter(
                pl.col("swing_point_type") == SwingPointType.LOW.value
            ).tail(1)
            prev_top_tmp = tmp_df.filter(
                pl.col("swing_point_type") == SwingPointType.HIGH.value
            ).tail(1)

            prev_bottom_index = (
                prev_bottom_tmp.row(0, named=True)["index"]
                if not prev_bottom_tmp.is_empty()
                else 0
            )
            prev_top_index = (
                prev_top_tmp.row(0, named=True)["index"]
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
                if prev_bottom_left and prev_bottom_middle and prev_bottom_right
                else (None, None)
            )
            prev_top_high_price, prev_top_low_price = (
                _get_fractal_range(prev_top_left, prev_top_middle, prev_top_right)
                if prev_top_left and prev_top_middle and prev_top_right
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
                        )
                        prev_top_index = (
                            sbar_df.filter(
                                pl.col("high_price") == pl.col("high_price").max()
                            )
                            .select("index")
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
                        )
                        prev_bottom_index = (
                            sbar_df.filter(
                                pl.col("low_price") == pl.col("low_price").min()
                            )
                            .select("index")
                            .item()
                        )

                        self.sbar_manager.update_by_index(
                            prev_bottom_index, "swing_point_level", prev_bottom_level
                        )

            # region 数据源更新
            # 是分形，更新分形标识和分形级别，更新cbar_df数据源、更新SBarManager
            self.df_cbar = self.df_cbar.with_columns(
                [
                    pl.when(pl.col("index") == middle_bar["index"])
                    .then(pl.lit(swing_point_type))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(pl.col("index") == middle_bar["index"])
                    .then(pl.lit(swing_point_level.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
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
                    pl.when(is_bottom_fractal)
                    .then(pl.col("low_price").min() == pl.col("low_price"))
                    .otherwise(pl.col("high_price").max() == pl.col("high_price"))
                )
                .select("index")
                .item()
            )
            self.sbar_manager.update(
                [
                    pl.when(pl.col("index") == sbar_index)
                    .then(pl.lit(swing_point_type))
                    .otherwise(pl.col("swing_point_type"))
                    .alias("swing_point_type"),
                    pl.when(pl.col("index") == sbar_index)
                    .then(pl.lit(swing_point_level.value))
                    .otherwise(pl.col("swing_point_level"))
                    .alias("swing_point_level"),
                ]
            )
            # endregion
        else:  # 不是分形
            pass

    def current_swing(self) -> Swing | None:
        """
        当前波段（永远是未完成状态）
        """
        start_index = self.get_current_swing_start_index()
        current_swing_df = self.df_cbar.slice(start_index)
        swing = _parse_swing(current_swing_df)
        return swing

    def prev_opposite_swing(self) -> Swing | None:
        """
        前一个与当前波段相反方向的波段
        """
        current_swing = self.current_swing()
        if current_swing.direction == SwingDirection.UP:
            prev_opposite_swing_point_type = SwingPointType.HIGH
        else:
            prev_opposite_swing_point_type = SwingPointType.LOW
        prev_opposite_swing_start_index = (
            self.df_cbar.slice(0, current_swing.index + 1)
            .filter(
                (pl.col("swing_point_type") == prev_opposite_swing_point_type)
                & (pl.col("swing_point_level") == SwingPointLevel.CURRENT_TIMEFRAME)
            )
            .select(pl.col("index").last())
            .item()
        )
        if not prev_opposite_swing_start_index:
            return None
        prev_opposite_swing_df = self.df_cbar.slice(
            prev_opposite_swing_start_index,
            current_swing.index - prev_opposite_swing_start_index + 1,
        )
        swing = _parse_swing(prev_opposite_swing_df)
        return swing

    def prev_same_swing(self):
        """
        前一个与当前波段相同方向的波段
        """
        prev_opposite_swing = self.prev_opposite_swing()
        if prev_opposite_swing is None:
            return None
        prev_same_swing_end_index = (
            self.df_cbar.filter(pl.col("index") == prev_opposite_swing.start_index)
            .select(pl.col("index").last())
            .item()
        )

        if prev_opposite_swing.direction == SwingDirection.UP:
            prev_same_swing_point_type = SwingPointType.HIGH
        else:
            prev_same_swing_point_type = SwingPointType.LOW

        prev_same_swing_start_index = (
            self.df_cbar.slice(0, prev_same_swing_end_index + 1)
            .filter(
                (pl.col("swing_point_type") == prev_same_swing_point_type)
                & (pl.col("swing_point_level") == SwingPointLevel.CURRENT_TIMEFRAME)
            )
            .select(pl.col("index").last())
            .item()
        )
        if not prev_same_swing_start_index:
            return None
        prev_same_swing_df = self.df_cbar.slice(
            prev_same_swing_start_index,
            prev_opposite_swing.start_index - prev_same_swing_start_index + 1,
        )
        swing = _parse_swing(prev_same_swing_df)
        return swing

    def compare_swings(self) -> (float, SwingDirection):
        """
        当前正在形成的波段占最近已完成波段的回调比例，>1说明已经突破前一波段，<1说明相对前一波段的回调比例
        """
        #
        # 当前波段最新价格与前一波段的关系分类：
        # 1. 在其中
        # 1.1 此时可以计算回调比例的关系
        # 2. 在其外
        # 2.1 已经超出前一波段的范围，说明已经突破了前波段高低点
        #
        current_swing = self.current_swing()
        if current_swing is None:
            return 0, SwingDirection.NONE
        last_price = (
            current_swing.high_price
            if current_swing.direction == SwingDirection.UP
            else current_swing.low_price
        )
        # 计算回调比例
        # 波段高低点距离
        prev_opposite_swing = self.prev_opposite_swing()
        if prev_opposite_swing is None:  # 之前没有波段
            return 0, SwingDirection.NONE

        if prev_opposite_swing.direction == SwingDirection.UP:  # 上升波段
            current_swing_distance = prev_opposite_swing.high_price - last_price
        else:
            # 下降波段
            current_swing_distance = last_price - prev_opposite_swing.low_price
        # 回调距离

        pullback_ratio = (
            current_swing_distance / prev_opposite_swing.distance
        )  # 当前正在形成的波段与前波段的关系，>1说明已经突破前一波段，<1说明相对前一波段的回调比例
        current_swing_direction = (
            SwingDirection.UP
            if prev_opposite_swing.direction == SwingDirection.DOWN
            else SwingDirection.DOWN
        )  # 当前正在形成波段的方向
        return pullback_ratio, current_swing_direction

    def get_current_swing_start_index(self) -> int:
        """
        取当前波段的开始索引
        :return:
        """
        # 取当前波段的开始索引
        start_index = (
            self.df_cbar.filter(
                (pl.col("swing_point_type") != SwingPointType.NONE)
                & (pl.col("swing_point_level") == SwingPointLevel.CURRENT_TIMEFRAME)
            )
            .tail(1)
            .select(pl.col("index"))
            .item()
        )
        return start_index


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


def _parse_swing(swing_df: pl.DataFrame) -> Swing | None:
    """
    解析Swing
    :param swing_df: 包含波段高低点的数据集，第一行为波段起点，最后一行为波段终点
    :return: Swing | None
    """
    if swing_df.is_empty():
        return None

    start_row = swing_df.row(0, named=True)
    end_row = swing_df.tail(1).row(0, named=True)

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
