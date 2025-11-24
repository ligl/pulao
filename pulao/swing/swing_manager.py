from typing import Any, List
from pulao.events import Observable
import polars as pl

from pulao.swing import Swing
from .swing import Swing
from ..constant import (
    EventType,
    SwingDirection,
    SwingPointType,
    SwingPointLevel,
    Const,
    FractalType,
)
from ..bar import CBarManager, CBar, Fractal
from ..logging import logger


class SwingManager(Observable):
    def __init__(self, cbar_manager: CBarManager):
        super().__init__()
        schema = {
            "index": pl.UInt32,
            "start_index": pl.UInt32,
            "end_index": pl.UInt32,  # 如果是active swing，end_index = 最新k线
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "direction": pl.Int8,
            "is_completed": pl.Boolean,  # 还未被确认的波段，即正在进行中的波段，在实时行情中尚未被确认
        }
        self.df_swing: pl.DataFrame = pl.DataFrame(schema=schema)
        self.cbar_manager: CBarManager = cbar_manager
        self.cbar_manager.subscribe(self._on_cbar_created)

    def _on_cbar_created(self, event: EventType, payload: Any):
        # 波段检测识别
        self._build_swing()

    def _build_swing(self):
        """
        构建波段
        """
        #
        # 波段
        # 原则：人看着是就是
        # 目标：符合交易员在图表中人为划分的尺度
        # 定义：某周期看起来有明显方向的、没有明显回调的一段走势，由推动力量和反抗力量的合力决定，所以既要考虑结构本身还要考虑幅度。
        # 判定标准:
        # 1. 由相邻的两个顶底分形连接而成（顶→底 或 底→顶）
        # 2. 分形之间不能重叠
        # 3. 被确认的波段有可能被打破，如上升波段成立后，在顶分形没有向下发展出下降笔，而是又转头向上，创出新高形成新的顶分形，则之前的顶分形作废，换成新的
        # 执行流程：
        # 1. 新的一条cbar被创建
        # 2. 取最后3条cbar做分形判断
        # 3. 如果构成分形
        # 3.1 如果是第一条swing，此分形即是swing起点，直接添加到df_swing
        # 3.2 如果不是第一条swing，此时就是尚未完成，正在进行的波段active swing
        # 3.2.1 判断分形的类型与active swing的方向关系
        # 3.2.2 如果在下降波段中出现顶分形或上升波段中出现底分形，说明波段延续
        #
        #
        cbar_list = self.cbar_manager.get_last_cbar(3)
        if cbar_list is None or len(cbar_list) != 3:
            logger.debug(
                "用于组成分形的cbar数量不够",
                cbar_count=len(cbar_list) if cbar_list is not None else None,
            )
            return
        curr_fractal = Fractal(
            left=cbar_list[0], middle=cbar_list[1], right=cbar_list[2]
        )
        fractal_type = curr_fractal.valid()
        # 情况1，最后3根bar组成不了分形
        if fractal_type == FractalType.NONE:
            if self.df_swing.is_empty():  # 不是分形，且已有波段，延长active swing
                # 不是分形，又没有波段，不符合条件，丢弃
                return
            active_swing = self.get_active_swing()
            active_swing.end_index = curr_fractal.end_index
            active_swing.high_price = max(
                active_swing.high_price, curr_fractal.high_price
            )
            active_swing.low_price = min(active_swing.low_price, curr_fractal.low_price)
            self._update_active_swing(
                index=active_swing.index,
                direction=active_swing.direction,
                start_index=active_swing.start_index,
                end_index=active_swing.end_index,
                high_price=active_swing.high_price,
                low_price=active_swing.low_price,
                is_completed=active_swing.is_completed,
            )
            return  # 不是分形，延续现有波段

        # 情况2，最后3根bar能组成分形
        # 1). 第一次构建，直接添加
        if self.df_swing.is_empty():
            self._append_swing(
                direction=SwingDirection.DOWN
                if fractal_type == FractalType.TOP
                else SwingDirection.UP,
                start_index=curr_fractal.index,
                end_index=curr_fractal.end_index,
                high_price=curr_fractal.high_price,
                low_price=curr_fractal.low_price,
                is_completed=False,
            )
            return

        # 2). 已经有波段，判断是延续还是终结波段
        last_swing = self.get_swing()
        active_swing = self.get_active_swing()

        active_swing_start_fractal = self.cbar_manager.get_fractal(
            active_swing.start_index
        )

        active_swing.high_price = max(active_swing.high_price, curr_fractal.high_price)
        active_swing.low_price = min(active_swing.low_price, curr_fractal.low_price)

        if self._valid_swing(
            curr_fractal, active_swing_start_fractal, prev_swing=last_swing
        ):  # 两个分形可以组成一个波段
            # 下降波段中出顶分形或上升波段中出现底分形，说明波段在延续
            active_swing.end_index = curr_fractal.index  # 波段完成时
            active_swing.is_completed = True
        else:
            # 不能组成波段，即波段延续延续
            active_swing.end_index = curr_fractal.end_index  # 波段未完成，记录最新k
            active_swing.is_completed = False

        self._update_active_swing(
            index=active_swing.index,
            direction=active_swing.direction,
            start_index=active_swing.start_index,
            end_index=active_swing.end_index,
            high_price=active_swing.high_price,
            low_price=active_swing.low_price,
            is_completed=active_swing.is_completed,
        )

    def _append_swing(
        self,
        direction: SwingDirection,
        start_index: int,
        end_index: int,
        high_price: float,
        low_price: float,
        is_completed: bool,
    ):
        new_swing = {
            "index": self.df_swing.height,
            "direction": direction.value,
            "start_index": start_index,
            "end_index": end_index,
            "high_price": high_price,
            "low_price": low_price,
            "is_completed": is_completed,
        }
        self.df_swing = self.df_swing.vstack(
            pl.DataFrame([new_swing], schema=self.df_swing.schema)
        )

    def _update_active_swing(
        self,
        index: int,
        direction: SwingDirection,
        start_index: int,
        end_index: int,
        high_price: float,
        low_price: float,
        is_completed: bool,
    ):
        # 如果最新的波段未完成，才需要删除，否则直接插入
        if self.df_swing.height > 0:
            self.df_swing = self.df_swing.slice(
                0, self.df_swing.height - 1
            )  # 删除原active swing，即最后一行
        self._append_swing(
            direction, start_index, end_index, high_price, low_price, is_completed
        )

    def _valid_swing(
        self, start_fractal: Fractal, end_fractal: Fractal, prev_swing: Swing = None
    ) -> bool:
        """
        判断两个分形是否能够组成笔
        """
        if start_fractal is None or end_fractal is None:
            logger.error("在调用_valid_swing方法时，参数值有None")
            raise AssertionError("start_fractal and end_fractal are both None")

        if (
            start_fractal.valid() == end_fractal.valid()
        ):  # 同向分形不可能组成波段，必须是不同向分形才行
            return False

        if not start_fractal.overlap(end_fractal):  # 两个分形没有重叠可形成笔
            return True
        # TODO 可以添加波动率为准绳的条件
        # 如果分形之间有重叠，
        # 1）但两分形中间cbar并未重叠，
        # 2）且已经超过了前一波段的60%，
        # 3）并且在包含合并处理之前的sbar中，间隔超过5根K线，
        # 那么，也视为波段成立（因为反抗力量确实也足够）
        if prev_swing is None:
            return False
        if start_fractal.overlap(end_fractal, is_strict=False):
            return False
        distance = max(start_fractal.high_price, end_fractal.high_price) - min(
            start_fractal.low_price, end_fractal.low_price
        )
        if distance / prev_swing.distance < 0.6:
            return False
        start_index_sbar = start_fractal.middle.end_index
        end_index_sbar = end_fractal.middle.start_index
        count_between = abs(end_index_sbar - start_index_sbar) - 1
        if count_between < 3:
            return False
        return True

    def get_active_swing(self) -> Swing | None:
        """
        获取未完成的波段
        :return: Swing | None
        """
        last_swing = self.get_swing()
        if not last_swing:
            return None
        if last_swing.is_completed:
            # 如果当前波段已经完成，则以终止点们起点构建active_swing
            last_swing_end_fractal = self.cbar_manager.get_fractal(last_swing.end_index)
            if not last_swing_end_fractal:
                raise AssertionError("此处不应该被执行，如果被执行，说明数据源错误。")

            active_swing = Swing(
                index=last_swing.index + 1,
                direction=last_swing.opposite_direction,
                start_index=last_swing_end_fractal.index,
                end_index=last_swing_end_fractal.end_index,
                high_price=last_swing_end_fractal.high_price,
                low_price=last_swing_end_fractal.low_price,
                is_completed=False,
            )
        else:
            # 如果未完成，那么最后一个波段就是active swing
            active_swing = last_swing
        return active_swing

    def get_swing(self, index: int = None) -> Swing | None:
        """
        获取指定波段
        :param index: 指定index开始的波段，如果没有指定，获取最新波段
        :return: Swing | None
        """
        if index is None:
            index = self.df_swing.height - 1
        if self.df_swing.height <= index or self.df_swing.height == 0:
            return None
        return Swing(**self.df_swing.row(index, named=True))

    def prev_opposite_swing(self, index: int) -> Swing | None:
        """
        前一个与指定波段相反方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """

        raise NotImplementedError("未实现")

    def prev_same_swing(self, index: int) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """

        raise NotImplementedError("未实现")

    def next_opposite_swing(self, index: int) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        raise NotImplementedError("未实现")

    def next_same_swing(self, index: int) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        raise NotImplementedError("未实现")

    def prev_swing(self, index: int) -> Swing | None:
        """
        查指定波段的前一个波段（与prev_opposite_swing等效）
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        return self.prev_opposite_swing(index)

    def next_swing(self, index: int) -> Swing | None:
        """
        查指定波段的后一个波段（与next_opposite_swing等效）
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        return self.next_opposite_swing(index)

    def get_swing_list(
        self, start_index: int, end_index: int, include_active: bool = True
    ) -> list[Swing] | None:
        """
        获取波段列表
        :param start_index:
        :param end_index:
        :param include_active: 是否包含active swing
        :return:
        """
        df = self.df_swing.slice(start_index, end_index - start_index + 1)
        if not include_active:
            df = df.filter((pl.col("is_completed") != True))
        if df.is_empty():
            return None
        return [Swing(**row) for row in df.rows(named=True)]
