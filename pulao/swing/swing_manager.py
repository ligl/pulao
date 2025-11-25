from typing import Any

import polars as pl

from pulao.events import Observable
from .swing import Swing
from ..bar import CBarManager, Fractal
from ..constant import (
    EventType,
    SwingDirection,
    FractalType,
)
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
        # 1. 接收到一条cbar被创建
        # 2. 情况1，未创建过波段，且最后3根bar组成不了分形
        # 3. 情况2，未创建过波段，且最后3根bar能组成分形，说明第一次创建波段起点
        # 4. 情况3，已经有波段，判断是延续还是终结波段
        #
        #
        cbar_list = self.cbar_manager.get_last_cbar(3)
        if cbar_list is None or len(cbar_list) != 3:
            logger.warning(
                "用于组成分形的cbar数量不够",
                cbar_count=len(cbar_list) if cbar_list is not None else 0,
            )
            return

        left_bar, middle_bar, right_bar = cbar_list

        curr_fractal = Fractal(left=left_bar, middle=middle_bar, right=right_bar)
        fractal_type = curr_fractal.valid()
        # 情况1，未创建过波段，且最后3根bar组成不了分形
        # 情况2，未创建过波段，且最后3根bar能组成分形，说明第一次创建波段起点
        # 首次波段创建
        if self.df_swing.is_empty():
            if fractal_type == FractalType.NONE:
                # 不是分形，又没有波段，不符合条件，丢弃
                logger.info(
                    "情况1，最后3根bar组成不了分形，又没有波段，不符合条件，丢弃"
                )
                return
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
            logger.debug(
                "情况2，最后3根bar能组成分形。第一次构建，直接添加。",
                fractal=curr_fractal
            )
            return

        # 情况3，已经有波段，判断是延续还是终结波段
        last_swing = self.get_swing()
        active_swing = self.get_active_swing()
        if active_swing is None:
            # 说明当前波段已经完成，需要以终止点为新的起点构建active_swing
            last_swing_end_fractal = self.cbar_manager.get_fractal(last_swing.end_index)
            if not last_swing_end_fractal:
                raise AssertionError("数据源异常，无法获取last_swing_end_fractal")

            active_swing = Swing(
                index=last_swing.index + 1,
                direction=last_swing.opposite_direction,
                start_index=last_swing_end_fractal.index,
                end_index=last_swing_end_fractal.end_index, # 此时，波段处于未完成状态，end_index为最新bar的索引，并不是分形顶底bar
                high_price=last_swing_end_fractal.high_price,
                low_price=last_swing_end_fractal.low_price,
                is_completed=False,
            )
            self._append_swing(
                direction=active_swing.direction,
                start_index=active_swing.start_index,
                end_index=active_swing.end_index,
                high_price=active_swing.high_price,
                low_price=active_swing.low_price,
                is_completed=active_swing.is_completed,
            )
            return
        active_swing_start_fractal = self.cbar_manager.get_fractal(
            active_swing.start_index
        )

        # 更新 active swing 价格
        active_swing.high_price = max(active_swing.high_price, curr_fractal.high_price)
        active_swing.low_price = min(active_swing.low_price, curr_fractal.low_price)

        if self._valid_swing(
            start_fractal=active_swing_start_fractal,
            end_fractal=curr_fractal,
            active_swing=active_swing,
            prev_swing=last_swing,
        ):  # 两个分形可以组成一个波段
            # 下降波段中出顶分形或上升波段中出现底分形，说明波段在延续
            active_swing.end_index = curr_fractal.index  # 波段完成时
            active_swing.is_completed = True
            logger.debug(
                "情况3，最后3根bar能组成分形。且完结波段。",
                active_swing=active_swing,
                df_swing_height=self.df_swing.height,
            )
        else:
            # 不能组成波段，即波段延续延续
            active_swing.end_index = curr_fractal.end_index  # 波段未完成，记录最新k
            active_swing.is_completed = False
            logger.debug(
                "情况3，最后3根bar能组成分形。且与之前波段同向，延续波段。",
                active_swing=active_swing,
                df_swing_height=self.df_swing.height,
            )

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
        self,
        start_fractal: Fractal,
        end_fractal: Fractal,
        active_swing: Swing,
        prev_swing: Swing = None,
    ) -> bool:
        """
        判断两个分形是否能够组成笔
        """
        if start_fractal is None or end_fractal is None:
            logger.error("_valid_swing 在调用_valid_swing方法时，参数值有None")
            raise AssertionError("start_fractal and end_fractal are both None")

        if (
            start_fractal.valid() == end_fractal.valid()
        ):  # 同向分形不可能组成波段，必须是不同向分形才行
            logger.debug("_valid_swing 相邻同向分形，不能构成分形")
            return False

        if not start_fractal.overlap(end_fractal):  # 两个分形没有重叠可形成笔
            logger.debug("_valid_swing 两个分形没有重叠可形成笔")
            return True
        # TODO 可以添加波动率为准绳的条件
        # 如果分形之间有重叠，
        # 1）但两分形中间cbar并未重叠，
        # 2）且已经超过了前一波段的60%，
        # 3）并且在包含合并处理之前的sbar中，间隔超过5根K线，
        # 那么，也视为波段成立（因为反抗力量确实也足够）
        if prev_swing is None:
            logger.debug("_valid_swing prev_swing为None，波动率比较取消")
            return False
        if start_fractal.overlap(end_fractal, is_strict=False):
            return False
        distance = max(start_fractal.high_price, end_fractal.high_price) - min(
            start_fractal.low_price, end_fractal.low_price
        )
        if distance / prev_swing.distance < 0.6:
            return False
        start_index_sbar = start_fractal.middle.index
        end_index_sbar = end_fractal.middle.index
        count_between = abs(end_index_sbar - start_index_sbar) - 1
        if count_between < 5:
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
            # 最后一个波段已经完成
            return None
        else:
            # 如果未完成，那么最后一个波段就是active swing
            return last_swing

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
        if index <= 0 or index >= self.df_swing.height:
            return None
        return Swing(**self.df_swing.row(index - 1, named=True))


    def prev_same_swing(self, index: int) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        return self.prev_opposite_swing(index - 1)

    def next_opposite_swing(self, index: int) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        if index < 0 or index >= self.df_swing.height - 1:
            return None
        return Swing(**self.df_swing.row(index + 1, named=True))

    def next_same_swing(self, index: int) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param index: 指定index所在的波段
        :return: Swing | None
        """
        return self.next_opposite_swing(index + 1)

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
