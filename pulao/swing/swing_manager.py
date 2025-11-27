from typing import Any, List

import polars as pl
from datetime import datetime as Datetime

from pulao.events import Observable
from .swing import Swing
from ..bar import CBarManager, Fractal, CBar
from ..constant import (
    EventType,
    Direction,
    FractalType, Const,
)
from ..logging import logger
from ..utils import IDGenerator


class SwingManager(Observable):
    def __init__(self, cbar_manager: CBarManager):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "start_id": pl.UInt64, # df_cbar id
            "end_id": pl.UInt64,  # 如果是active swing，end_id = 最新k线
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "direction": pl.Int8,
            "is_completed": pl.Boolean,  # 还未被确认的波段，即正在进行中的波段，在实时行情中尚未被确认
            "created_at": pl.Datetime("ms"),
        }
        self.df_swing: pl.DataFrame = pl.DataFrame(schema=schema)
        self.cbar_manager: CBarManager = cbar_manager
        self.cbar_manager.subscribe(self._on_cbar_created)
        self.id_gen = IDGenerator()

    def _on_cbar_created(self, event: EventType, payload: Any):
        # 波段检测识别
        # logger.debug("_on_cbar_created", payload=payload)
        # if payload["backtrack_id"] is None:
        #     self._build_swing()
        # else:
        #     self._clean_reset(payload["backtrack_id"])
        #     self._backtrack_replay(payload["backtrack_id"])
        self._build_swing()

    def _clean_reset(self, traceback_id: int):
        # 1. 清理df_swing
        df = self.df_swing.filter(
            (pl.col("start_id") <= traceback_id) & (traceback_id <= pl.col("end_id")))
        if df.is_empty():
            return
        # 只有一种情况会出现两条数据，即traceback_id是一个波的终点，同时又是另一个波段的起点
        first_swing = Swing(**df.row(0, named=True))
        first_swing_index = self.get_index(first_swing.id)
        if df.height > 1:
            # 删除traceback_id之后的数据
            logger.debug("_clean_swing 删除traceback_id之后的数据", traceback_id=traceback_id,
                         first_swing=first_swing, first_swing_index=first_swing_index)
            self.df_swing = self.df_swing.slice(0, first_swing_index + 1)
        if first_swing.start_id == traceback_id:  # 说明只有一个波段的时候
            logger.debug("_clean_swing 清空波段，重新构建", first_swing=first_swing,
                         first_swing_index=first_swing_index)
            self.df_swing = self.df_swing.slice(0, first_swing_index)
        else:
            # 在波段的中间
            first_swing.is_completed = False
            cbar = self.cbar_manager.get_nearby_cbar(traceback_id, -1)
            first_swing.end_id = cbar.id if cbar else None
            self._update_active_swing(id=first_swing.id, direction=first_swing.direction,
                              start_id=first_swing.start_id, end_id=first_swing.end_id,
                              high_price=first_swing.high_price, low_price=first_swing.low_price,
                              is_completed=first_swing.is_completed)

    def _backtrack_replay(self, backtrack_id:int = None):
        """
        查找要从哪个cbar开始回放处理，取值min(backtrack_id, swing.end_id)
        """
        # 2. 获取需要处理的cbar[backtrack_id, last_id]，进行回放
        cbar_list = self.cbar_manager.get_nearby_cbar(backtrack_id)
        if cbar_list is None:
            return
        for cbar in cbar_list:
            self._build_swing(cbar)

    def _build_swing(self, cbar: CBar = None):
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
        if cbar is None:
            cbar_list = self.cbar_manager.get_last_cbar(3) # 不需要回放，直接取最新的处理
        else:
            cbar_list = self.cbar_manager.get_nearby_cbar(cbar.id, -3)  # 有需要回话的情况

        if cbar_list is None or len(cbar_list) != 3:
            logger.warning(
                "用于组成分形的cbar数量不够",
                cbar_count=len(cbar_list) if cbar_list else 0,
            )
            return

        left_bar , middle_bar , right_bar = cbar_list
        last_bar = right_bar

        curr_fractal = Fractal(left=left_bar, middle=middle_bar, right=right_bar)

        fractal_type = curr_fractal.fractal_type()
        # 首次波段创建
        if self.df_swing.is_empty():
            if fractal_type == FractalType.NONE:
                # 不是分形，又没有波段，不符合条件，丢弃
                logger.info(
                    "最后3根bar组成不了分形，又没有已存在的波段，不符合条件，丢弃"
                )
                return
            # 以分形为基础创建波段起点
            self._append_swing(
                direction=Direction.DOWN
                if fractal_type == FractalType.TOP
                else Direction.UP,
                start_id=curr_fractal.id,
                end_id=curr_fractal.cbar_end_id,
                high_price=curr_fractal.high_price,
                low_price=curr_fractal.low_price,
                is_completed=False,
            )
            logger.debug(
                "首次波段创建",
                fractal=curr_fractal,
            )
            return

        # 已有波段，判断是延续还是终结波段
        last_completed_swing = self.get_swing(is_completed=True)

        # 在当前波段未终结前，任何一个时刻都有可能打破前完成波段，使其重新延续
        if last_completed_swing:
            if not last_completed_swing.is_completed:
                logger.error("不应该出现last_completed_swing is not completed", last_completed_swing=last_completed_swing)
                raise AssertionError("不应该出现last_completed_swing is not completed")

            if last_completed_swing.direction == Direction.DOWN:
                if last_bar.low_price < last_completed_swing.low_price:
                    # 新bar比下降波段的最低价还低，重新延续波段
                    last_completed_swing.end_id = last_bar.id
                    last_completed_swing.low_price = last_bar.low_price
                    last_completed_swing.is_completed = False

                    self._del_active_swing()
                    self._update_active_swing(id=last_completed_swing.id, direction=last_completed_swing.direction,
                                              start_id=last_completed_swing.start_id,
                                              end_id=last_completed_swing.end_id,
                                              high_price=last_completed_swing.high_price,
                                              low_price=last_completed_swing.low_price,
                                              is_completed=last_completed_swing.is_completed)
                    logger.debug("打开前一完成波段，延续", last_completed_swing=last_completed_swing, last_bar=last_bar)
                    return
            elif last_completed_swing.direction == Direction.UP:
                if last_bar.high_price > last_completed_swing.high_price:
                    last_completed_swing.end_id = last_bar.id
                    last_completed_swing.high_price = last_bar.high_price
                    last_completed_swing.is_completed = False

                    self._del_active_swing()
                    self._update_active_swing(id=last_completed_swing.id, direction=last_completed_swing.direction,
                                              start_id=last_completed_swing.start_id,
                                              end_id=last_completed_swing.end_id,
                                              high_price=last_completed_swing.high_price,
                                              low_price=last_completed_swing.low_price,
                                              is_completed=last_completed_swing.is_completed)
                    logger.debug("打开前一完成波段，延续", last_completed_swing=last_completed_swing, last_bar=last_bar)
                    return
            else:
                logger.error("不应该执行这里！波段方向取值不符合要求", last_completed_swing=last_completed_swing)
                raise AssertionError("不应该执行这里！波段方向取值不符合要求")

        active_swing = self.get_active_swing()
        if not active_swing and last_completed_swing:
            # 说明当前波段已经完成，需要以终止点为新的起点构建active_swing
            prev_swing_end_fractal = self.cbar_manager.get_fractal(last_completed_swing.end_id)
            if not prev_swing_end_fractal:
                logger.error(
                    "数据源异常，前一个波段终点分形异常", prev_swing=last_completed_swing
                )
                raise AssertionError("数据源异常，前一个波段终点分形异常")

            # 开始一个新波段
            active_swing = Swing(
                direction=last_completed_swing.opposite_direction,
                start_id=prev_swing_end_fractal.id,
                end_id=prev_swing_end_fractal.cbar_end_id,  # 此时，波段处于未完成状态，end_id为最新bar的索引，并不是分形顶底bar
                high_price=prev_swing_end_fractal.high_price,
                low_price=prev_swing_end_fractal.low_price,
                is_completed=False,
            )
            self._append_swing(
                direction=active_swing.direction,
                start_id=active_swing.start_id,
                end_id=active_swing.end_id,
                high_price=active_swing.high_price,
                low_price=active_swing.low_price,
                is_completed=active_swing.is_completed,
            )
            logger.debug("创建新波段", active_swing=active_swing, last_completed_swing=last_completed_swing)
            return

        # 更新 active swing 价格
        active_swing.high_price = max(active_swing.high_price, curr_fractal.high_price)
        active_swing.low_price = min(active_swing.low_price, curr_fractal.low_price)

        if self._valid_swing(
            start_fractal=self.cbar_manager.get_fractal(active_swing.start_id),
            end_fractal=curr_fractal,
            active_swing=active_swing,
            prev_swing=last_completed_swing,
        ):  # 两个分形可以组成一个波段
            # 下降波段中出顶分形或上升波段中出现底分形，说明波段在延续
            active_swing.end_id = curr_fractal.id  # 波段完成时
            active_swing.is_completed = True

            self._update_active_swing(
                id=active_swing.id,
                direction=active_swing.direction,
                start_id=active_swing.start_id,
                end_id=active_swing.end_id,
                high_price=active_swing.high_price,
                low_price=active_swing.low_price,
                is_completed=active_swing.is_completed,
            )
            logger.debug(
                "情况3，最后3根bar能组成分形。且完结波段。",
                active_swing=active_swing,
                prev_swing = last_completed_swing,
            )
        else:
            # 不能确认波段终结，即波段延续
            active_swing.end_id = curr_fractal.cbar_end_id  # 波段未完成，记录最新k
            active_swing.is_completed = False

            self._update_active_swing(
                id=active_swing.id,
                direction=active_swing.direction,
                start_id=active_swing.start_id,
                end_id=active_swing.end_id,
                high_price=active_swing.high_price,
                low_price=active_swing.low_price,
                is_completed=active_swing.is_completed,
            )
            # logger.debug(
            #     "情况3，最后3根bar能组成分形。且与之前波段同向，延续波段。",
            #     active_swing=active_swing,
            # )
    def _del_active_swing(self):
        active_swing = self.get_active_swing()
        if active_swing:
            self.df_swing = self.df_swing.slice(
                0, self.df_swing.height - 1
            )  # 删除未完成的波段
    def _append_swing(
        self,
        direction: Direction,
        start_id: int,
        end_id: int,
        high_price: float,
        low_price: float,
        is_completed: bool
    ):
        new_swing = {
            "id": self.id_gen.get_id(),
            "direction": direction.value,
            "start_id": start_id,
            "end_id": end_id,
            "high_price": high_price,
            "low_price": low_price,
            "is_completed": is_completed,
            "created_at": Datetime.now(),
        }
        if is_completed:
            start_fractal = self.cbar_manager.get_fractal(start_id)
            end_fractal = self.cbar_manager.get_fractal(end_id)
            if not start_fractal or not end_fractal:
                logger.error(
                    "波段断定无效",
                    swing=new_swing,
                    start_fractal=start_fractal,
                    end_fractal=end_fractal,
                )
                raise AssertionError("波段断定无效")
        self.df_swing = self.df_swing.vstack(
            pl.DataFrame([new_swing], schema=self.df_swing.schema)
        )

    def _update_active_swing(
        self,
        id: int,
        direction: Direction,
        start_id: int,
        end_id: int,
        high_price: float,
        low_price: float,
        is_completed: bool,
    ):
        """
        更新逻辑：先删除旧数据，再添加新数据，每条数据的id都不同
        """
        # 先删除再添加
        if self.df_swing.height > 0:
            self.df_swing = self.df_swing.slice(
                0, self.df_swing.height - 1
            )  # 删除原active swing，即最后一行
        self._append_swing(
            direction, start_id, end_id, high_price, low_price, is_completed
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
            logger.error("_valid_swing 在调用_valid_swing方法时，参数值有None", active_swing=active_swing, prev_swing=prev_swing)
            raise AssertionError("start_fractal and end_fractal are both None")

        start_fractal_type = start_fractal.fractal_type()
        end_fractal_type = end_fractal.fractal_type()

        if (
            start_fractal_type == FractalType.NONE
            or end_fractal_type == FractalType.NONE
        ):
            # logger.debug("_valid_swing 波段端点并非有效分形")
            return False
        if (
            start_fractal_type == end_fractal_type
        ):  # 同向分形不可能组成波段，必须是不同向分形才行
            # logger.debug("_valid_swing 相邻同向分形，不能构成分形")
            return False

        if not start_fractal.overlap(end_fractal):  # 两个分形没有重叠可形成波段
            logger.debug("_valid_swing 两个分形没有重叠可形成波段", start_fractal=start_fractal, end_fractal=end_fractal, active_swing=active_swing, prev_swing=prev_swing)
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
        start_id_sbar = start_fractal.middle.id
        end_id_sbar = end_fractal.middle.id
        count_between = abs(end_id_sbar - start_id_sbar) - 1
        if count_between >= 5:
            return True

        return False

    def get_fractal(self, cbar_id: int) -> Fractal:
        return self.cbar_manager.get_fractal(cbar_id)

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

    def get_index(self, id: int) -> int:
        return self.df_swing.select(pl.col("id").search_sorted(id)).item()

    def get_swing(self, id: int = None, is_completed:bool = None) -> Swing | None:
        """
        获取指定波段
        :param id: 指定id的波段，如果没有指定，获取最新波段
        :param is_completed: None：不限制
        :return: Swing | None
        """
        if id is None:
            index = self.df_swing.height - 1 # 取最后一条
        else:
            index = self.get_index(id)

        if index is None or index < 0 or index > self.df_swing.height -1:
            return None

        swing =  Swing(**self.df_swing.row(index, named=True))
        # 有没有指定id
        if id:
            if is_completed is None:
                return swing
            else:
                return swing if swing.is_completed == is_completed else None
        else:
            # 没有指定id
            if is_completed is None: # a. 对状态没有要求
                return swing
            else:
                if swing.is_completed == is_completed: # b. 要求的状态正好与最后一条吻合
                    return swing
                else: # c. 要求的状态与最后一条不吻合，查找最新的一条满足条件的数据
                    df = self.df_swing.tail(Const.LOOKBACK_LIMIT).filter(pl.col("is_completed") == is_completed).tail(1)
                    if df.is_empty():
                        return None
                    return Swing(**df.row(0, named=True))

    def prev_opposite_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        if id <= 0 or id >= self.df_swing.height:
            return None
        return Swing(**self.df_swing.row(id - 1, named=True))

    def prev_same_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        return self.prev_opposite_swing(id - 1)

    def next_opposite_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        if id < 0 or id >= self.df_swing.height - 1:
            return None
        return Swing(**self.df_swing.row(id + 1, named=True))

    def next_same_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        return self.next_opposite_swing(id + 1)

    def prev_swing(self, id: int) -> Swing | None:
        """
        查指定波段的前一个波段（与prev_opposite_swing等效）
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        return self.prev_opposite_swing(id)

    def next_swing(self, id: int) -> Swing | None:
        """
        查指定波段的后一个波段（与next_opposite_swing等效）
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        return self.next_opposite_swing(id)

    def get_swing_list(
        self, start_id: int, end_id: int, include_active: bool = True
    ) -> List[Swing] | None:
        """
        获取波段列表
        :param start_id:
        :param end_id:
        :param include_active: 是否包含active swing
        :return:
        """
        df = self.df_swing.slice(start_id, end_id - start_id + 1)
        if not include_active:
            df = df.filter((pl.col("is_completed") == True))
        if df.is_empty():
            return None
        return [Swing(**row) for row in df.rows(named=True)]
