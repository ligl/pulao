from typing import Any, Tuple, List

from pyexpat import features

from numpy.f2py.crackfortran import expr2name

from pulao.events import Observable
import polars as pl

from .swing import Swing
from ..constant import EventType, SwingDirection, SwingPointType, SwingPointLevel, Const
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
            "swing_point_level": pl.UInt8,  # 波段高低点级别（调整后的，正式用）
            "swing_point_level_origin": pl.UInt8,  # 波段高低点级别（原始级别）
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

    def _agg_bar(self, sbar: SBar):
        #
        # 对k线进行包含合并处理
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
            swing_point_level_origin = (
                SwingPointLevel.CURRENT_TIMEFRAME
            )  # 默认分形为本级别
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
            self.notify(EventType.SWING_CHANGED)
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
        # 2. 如果由次级别组成的波段，形成了一段上涨一段下跌的结构，则顶点调整为本级别，反之亦复如是
        # 3. 次级别组成的波段，没有趋势，形成了横盘区间，把区间的最低点（若未来价格离开区间向上）或最高点（若未来离开向下）调整为本级别
        # 4. 处理连续同级别同向分形，比如连续多个顶分形，调整最后一个为本级别，其他为次级别
        #
        # endregion

        # region 1. 如果相邻顶底分形有重叠区域，先调整为次级别
        # 取最新的一段数据进行重整，久远的数据级别已经固定，不用处理
        df_fractals = self.df_cbar.slice(
            self.df_cbar.height - Const.LOOKBACK_LIMIT
            if self.df_cbar.height - Const.LOOKBACK_LIMIT > 0
            else 0
        ).filter(pl.col("swing_point_type") != SwingPointType.NONE)

        cbar_list = []
        prev_row = None
        # 从新数据往旧数据遍历
        for i in range(df_fractals.height - 1, -1, -1):
            if prev_row is None:
                prev_row = df_fractals.row(i, named=True)
                continue
            curr_row = df_fractals.row(i, named=True)
            if _is_price_range_overlap(
                curr_row["high_price"],
                curr_row["low_price"],
                prev_row["high_price"],
                prev_row["low_price"],
            ):
                # 有重叠，修改后者为次级别（前者更可能先被人眼认定）
                curr_row["swing_point_level"] = SwingPointLevel.LOWER_TIMEFRAME.value
                cbar_list.append(curr_row)
            prev_row = curr_row
        if cbar_list:
            self.update_swing_point_level(cbar_list)

        # endregion

    def get_swing(self, index: int = None) -> Swing | None:
        """
        获取指定波段
        :param index: 指定index开始的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        if index is None:
            start_index = self.get_current_swing_start_index()
            end_index = self.df_cbar.height - 1
        else:
            start_index = index
            start_swing_point_type = (
                self.df_cbar.filter(pl.col("index") == start_index)
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
                self.df_cbar.slice(start_index, Const.LOOKBACK_LIMIT)
                .filter(
                    (pl.col("swing_point_type") == end_swing_point_type)
                    & (pl.col("swing_point_level") == SwingPointLevel.CURRENT_TIMEFRAME)
                )
                .select(pl.col("index").last())
                .item()
            )
            end_index = (
                self.df_cbar.height - 1 if not end_index else end_index
            )  # 如果没有查到波段终点，说明波段并未结束
        current_swing_df = self.df_cbar.slice(start_index, end_index - start_index + 1)
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
            self.df_cbar.slice(
                slice_index, prev_opposite_swing_end_index - slice_index + 1
            )
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
            self.df_cbar.slice(slice_index, prev_same_swing_end_index - slice_index + 1)
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
        return self.df_cbar.slice(start_index, end_index - start_index + 1).filter(
            (pl.col("swing_point_type") != SwingPointType.NONE)
            & (pl.col("swing_point_level") == SwingPointLevel.CURRENT_TIMEFRAME)
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
        current_swing = self.get_swing()
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
            .select(pl.col("index").last())
            .item()
        )
        return start_index

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

            expr_sbar = pl.when(pl.col("index") == bar_index).then(
                pl.lit(cbar["swing_point_level"])).otherwise(expr_sbar)
            expr_cbar = pl.when(pl.col("index") == cbar["index"]).then(
                pl.lit(cbar["swing_point_level"])).otherwise(expr_cbar)

            # 1. 更新数据源-sbar_df
            self.sbar_manager.update(expr_sbar.alias("swing_point_level"))
            # 2. 更新数据源-cbar_df
            self.df_cbar = self.df_cbar.with_columns(expr_cbar.alias("swing_point_level"))


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
