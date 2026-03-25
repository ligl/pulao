from typing import Any, List, Literal

import polars as pl
from datetime import datetime as Datetime

from pulao.events import Observable
from .swing import Swing, SwingState
from pulao.bar import CBar, CBarManager, Fractal
from pulao.constant import (
    EventType,
    Direction,
    FractalType,
    Const,
    Timeframe,
)
from pulao.logging import get_logger
from pulao.utils import IDGenerator

logger = get_logger(__name__)


class SwingManager(Observable):
    def __init__(self, cbar_manager: CBarManager):
        super().__init__()
        schema = {
            "id": pl.UInt64,
            "cbar_start_id": pl.UInt64,  # df_cbar id
            "cbar_end_id": pl.UInt64,  # 如果是active swing，end_id = 最新k线
            "sbar_start_id": pl.UInt64,  # df_sbar id
            "sbar_end_id": pl.UInt64,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "direction": pl.Int8,
            "span": pl.UInt32,
            "volume": pl.Float32,
            "start_oi": pl.Float32,
            "end_oi": pl.Float32,
            "state": pl.Int8,  # 还未被确认的波段，即正在进行中的波段，在实时行情中尚未被确认
            "created_at": pl.Datetime("ms"),
        }
        self.df_swing: pl.DataFrame = pl.DataFrame(schema=schema)
        self.cbar_manager: CBarManager = cbar_manager
        self.cbar_manager.subscribe(self._on_cbar_changed, EventType.CBAR_CHANGED)
        self.id_gen = IDGenerator(worker_id=4)
        self.backtrack_id: int | None = (
            None  # swing变动之后，告诉订阅者，从哪个swing id开始重新计算，大于等于此id的都要被重新计算
        )
        self.symbol = self.cbar_manager.symbol
        self.timeframe = self.cbar_manager.timeframe
        self.swing_builder = _SwingBuilder(self)

    def _on_cbar_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        self.backtrack_id = None
        cbar_backtrack_id = payload.get("backtrack_id", None)
        # 波段检测识别

        # logger.debug("_on_cbar_created", payload=payload)
        if cbar_backtrack_id is None:
            self.swing_builder._build_swing()
        else:
            self.swing_builder._clean_backtrack(cbar_backtrack_id)
            self.swing_builder._backtrack_replay(cbar_backtrack_id)
        self.write_parquet()
        self.notify(timeframe, EventType.SWING_CHANGED, backtrack_id=self.backtrack_id)

    def write_parquet(self):
        # TODO 实时行情不能这么做，需要考虑性能影响
        self.df_swing.write_parquet(
            Const.PARQUET_PATH.format(symbol=self.symbol, filename=f"swing_{self.timeframe}"),
            compression="zstd",
            compression_level=3,
            statistics=False,
            mkdir=True,
        )

    def read_parquet(self):
        self.df_swing = pl.read_parquet(
            Const.PARQUET_PATH.format(symbol=self.symbol, filename=f"swing_{self.timeframe}")
        )
        return self.df_swing

    def get_fractal(self, cbar_id: int) -> Fractal:
        return self.cbar_manager.get_fractal(cbar_id)

    def get_limit_swing(
        self, start_id: int, end_id: int, arg: Literal["max", "min"], direction: Direction
    ) -> Swing | None:
        """
        获取一段区间[start_id, end_id]中在某个方向的最高价或最低价的波段，即max(high_price)或min(low_price)
        :param start_id:
        :param end_id:
        :param arg: max or min
        :param direction:
        :return:
        """
        if arg not in ["max", "min"]:
            return None
        start_index = self.get_index(start_id)
        end_index = self.get_index(end_id)
        if start_index is None or end_index is None:
            return None
        if start_index > end_index:  # 交换
            start_index, end_index = end_index, start_index

        df = self.df_swing.slice(start_index, end_index - start_index + 1).filter(pl.col("direction") == direction)
        if df.is_empty():
            return None
        if arg == "max":
            index = df["high_price"].arg_max()
        else:
            index = df["low_price"].arg_min()
        return Swing(**df.row(index, named=True))

    def get_limit_swing_id(
        self, start_id: int, end_id: int, arg: Literal["max", "min"], direction: Direction
    ) -> int | None:
        swing = self.get_limit_swing(start_id, end_id, arg, direction)
        if swing is None:
            return None
        return swing.id

    def get_index(self, id: int) -> int | None:
        idx = self.df_swing.select(pl.col("id").search_sorted(id)).item()
        if idx >= self.df_swing.height or self.df_swing["id"][idx] != id:
            return None
        else:
            return idx

    def get_nearest_swing(self, id: int, count: int | None = None) -> None | Swing | List[Swing]:
        """
        获取指定id向前/向后 count个swing
        :param id:
        :param count: 正数向后，负数向前，None:获取到结尾
        :return:
        """
        idx = self.df_swing.select(pl.col("id").search_sorted(id)).item()
        if idx is None or idx >= self.df_swing.height:
            return None

        is_return_single = (count == 1 or count == -1) if count else False

        if count is None:
            count = self.df_swing.height - 1
        if count < 0:  # 向前
            count = -count  # 变成正数
            end_index = idx - 1
            start_index = end_index - count + 1
        else:  # 向后
            start_index = idx + 1
            end_index = start_index + count - 1

        if start_index < 0:
            start_index = 0
            end_index = idx - 1
        if end_index < 0:
            return None

        df = self.df_swing.slice(start_index, end_index - start_index + 1)
        if df.is_empty():
            return None

        if is_return_single:
            return Swing(**df.row(0, named=True))

        return [Swing(**row) for row in df.rows(named=True)]

    def get_last_swing(self) -> None | Swing:

        df = self.df_swing.tail(1)
        if df.is_empty():
            return None
        return Swing(**df.row(0, named=True))

    def get_swing(self, id: int | None = None, is_completed: bool | None = None) -> Swing | None:
        """
        获取指定波段
        :param id: 指定id的波段，如果没有指定，获取最新波段
        :param is_completed: None：不限制
        :return: Swing | None
        """
        if id is None:
            index = self.df_swing.height - 1  # 取最后一条
        else:
            index = self.get_index(id)

        if index is None or index < 0 or index > self.df_swing.height - 1:
            return None

        swing = Swing(**self.df_swing.row(index, named=True))
        # 有没有指定id
        if id:
            if is_completed is None:
                return swing
            else:
                return swing if swing.is_completed == is_completed else None
        else:
            # 没有指定id
            if is_completed is None:  # a. 对状态没有要求
                return swing
            else:
                if swing.is_completed == is_completed:  # b. 要求的状态正好与最后一条吻合
                    return swing
                else:  # c. 要求的状态与最后一条不吻合，查找最新的一条满足条件的数据
                    df = self.df_swing.tail(Const.LOOKBACK_LIMIT).filter(pl.col("is_completed") == is_completed).tail(1)
                    if df.is_empty():
                        return None
                    return Swing(**df.row(0, named=True))

    def get_swing_by_index(self, index: int) -> Swing | None:
        if index is None or index <= 0 or index >= self.df_swing.height - 1:
            return None
        return Swing(**self.df_swing.row(index, named=True))

    def prev_opposite_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_swing_by_index(index - 1)

    def prev_same_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_swing_by_index(index - 2)

    def next_opposite_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_swing_by_index(index + 1)

    def next_same_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        index = self.get_index(id)
        if index is None:
            return None
        return self.get_swing_by_index(index + 2)

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

    def get_swing_list(self, start_id: int, end_id: int, include_active: bool = True) -> List[Swing] | None:
        """
        获取[start_id,end_id]之间的波段列表
        :param start_id:
        :param end_id:
        :param include_active: 是否包含active swing
        :return:
        """
        start_index = self.get_index(start_id)
        end_index = self.get_index(end_id)
        if start_index is None or end_index is None:
            return None
        if start_index > end_index:  # 交换
            start_index, end_index = end_index, start_index

        df = self.df_swing.slice(start_index, end_index - start_index + 1)
        if not include_active:
            df = df.filter((pl.col("is_completed") == True))
        if df.is_empty():
            return None
        return [Swing(**row) for row in df.rows(named=True)]

    def pretty_worker_id(self):
        return self.df_swing.with_columns(
            [
                ((pl.col("id") // (2**12)) % (2**10)).alias("worker_id_swing"),
                ((pl.col("cbar_start_id") // (2**12)) % (2**10)).alias("worker_id_cbar_start"),
                ((pl.col("cbar_end_id") // (2**12)) % (2**10)).alias("worker_id_cbar_end"),
                ((pl.col("sbar_start_id") // (2**12)) % (2**10)).alias("worker_id_sbar_start"),
                ((pl.col("sbar_end_id") // (2**12)) % (2**10)).alias("worker_id_sbar_end"),
            ]
        )


class _SwingDiscover:
    """
    波段发现器，负责根据k线数据发现波段
    """

    def __init__(self, swing_builder: _SwingBuilder):
        self.swing_builder = swing_builder
        self.bottom_fractal: Fractal | None = None
        self.top_fractal: Fractal | None = None

    def discover(self, fractal: Fractal) -> dict | None:
        """
        根据当前分形发现波段
        :param fractal:
        :return:
        """
        fractal_type = fractal.fractal_type()
        fractal_type = fractal.fractal_type()
        if fractal_type == FractalType.NONE:
            # 不是分形，又没有波段，不符合条件，丢弃
            logger.info("最后3根bar组成不了分形，又没有已存在的波段，不符合条件，丢弃")
            return
        # new_swing = {
        #     "direction": direction,
        #     "cbar_start_id": fractal.id,
        #     "cbar_end_id": fractal.cbar_end_id,
        #     "high_price": fractal.high_price,
        #     "low_price": fractal.low_price,
        #     "sbar_start_id": sbar_start_id,
        #     "sbar_end_id": sbar_end_id,
        #     "state": SwingState.Tentative,
        # }

        # 初始化状态
        if self.bottom_fractal is None and self.top_fractal is None:
            if fractal_type == FractalType.BOTTOM:
                self.bottom_fractal = fractal
            elif fractal_type == FractalType.TOP:
                self.top_fractal = fractal
        elif self.bottom_fractal is not None and self.top_fractal is None:
            # 如果已经有底分形了，说明在等待顶分形来确认波段
            if fractal_type == FractalType.BOTTOM:
                if fractal.low_price < self.bottom_fractal.low_price:
                    # 出现了更低的底分形，更新底分形
                    self.bottom_fractal = fractal
            elif fractal_type == FractalType.TOP:
                self.top_fractal = fractal
        elif self.top_fractal is not None and self.bottom_fractal is None:
            if fractal_type == FractalType.TOP:
                if fractal.high_price > self.top_fractal.high_price:
                    # 出现了更高的顶分形，更新顶分形
                    self.top_fractal = fractal
                elif fractal_type == FractalType.BOTTOM:
                    self.bottom_fractal = fractal
        else:
            # 已经同时有了顶分形和底分形，说明在等待新的分形来确认波段
            if fractal_type == FractalType.BOTTOM:
                if fractal.low_price < self.bottom_fractal.low_price:  # type: ignore
                    # 出现了更低的底分形，更新底分形
                    self.bottom_fractal = fractal
            elif fractal_type == FractalType.TOP:
                if fractal.high_price > self.top_fractal.high_price:  # type: ignore
                    # 出现了更高的顶分形，更新顶分形
                    self.top_fractal = fractal

        # 检测当前顶底分型是否能构成一个波段，如果能构成一个波段，则返回新波段信息，并重置状态
        if self.bottom_fractal and self.top_fractal:
            if self.bottom_fractal.cbar_end_id < self.top_fractal.cbar_end_id:  # 按照时间顺序确定波段方向
                direction = Direction.UP
                start_fractal = self.bottom_fractal
                end_fractal = self.top_fractal
            else:
                direction = Direction.DOWN
                start_fractal = self.top_fractal
                end_fractal = self.bottom_fractal

            is_swing = self.swing_builder.determine_swing(
                start_fractal=start_fractal,
                end_fractal=end_fractal,
                direction=direction,
                curr_swing=None,
                prev_swing=None,
            )  # 判断新波段是否成立
            if is_swing:
                new_swing = {
                    "direction": direction,
                    "cbar_start_id": start_fractal.id,
                    "cbar_end_id": end_fractal.id,
                    "high_price": max(start_fractal.high_price, end_fractal.high_price),
                    "low_price": min(start_fractal.low_price, end_fractal.low_price),
                    "sbar_start_id": self.swing_builder.swing_manager.cbar_manager.get_limit_sbar_id(
                        start_fractal.sbar_start_id,
                        start_fractal.sbar_end_id,
                        "max" if direction == Direction.UP else "min",
                    ),
                    "sbar_end_id": self.swing_builder.swing_manager.cbar_manager.get_limit_sbar_id(
                        end_fractal.sbar_start_id,
                        end_fractal.sbar_end_id,
                        "min" if direction == Direction.UP else "max",
                    ),
                    "state": SwingState.Tentative,
                }
            else:
                new_swing = None

            # clear state
            self.bottom_fractal = None
            self.top_fractal = None

            return new_swing


class _SwingBuilder:
    """
    波段构建器，负责根据k线数据构建波段
    """

    def __init__(self, swing_manager: SwingManager):
        self.swing_manager = swing_manager
        self.first_swing_discover = _SwingDiscover(self)  # 保存首次构建波段的探索状态

    def _build_swing(self, cbar: CBar | None = None):
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
        # 2. 情况1，未创建过波段，且最后3根bar组成不了分形，舍弃掉
        # 3. 情况2，未创建过波段，且最后3根bar能组成分形，说明第一次创建波段起点
        # 4. 情况3，已经有波段，判断是延续还是终结波段
        #
        #
        if cbar is None:
            cbar_list = self.swing_manager.cbar_manager.get_last_cbars(3)  # 不需要回放，直接取最新的处理
        else:
            cbar_list = self.swing_manager.cbar_manager.get_nearest_cbars(cbar.id, -3)  # 需要回放的情况

        if cbar_list is None or len(cbar_list) != 3:
            logger.info(
                "用于组成分形的cbar数量不够",
                cbar_count=len(cbar_list) if cbar_list else 0,
            )
            return

        left_bar, middle_bar, right_bar = cbar_list
        last_bar = right_bar

        curr_fractal = Fractal(left=left_bar, middle=middle_bar, right=right_bar)

        # 首次波段创建
        if self.swing_manager.df_swing.is_empty():
            return self._build_first_swing(curr_fractal)

        # 已有波段，判断是延续、候选待确认还是确认终结波段
        last_swing = self.swing_manager.get_last_swing()  # 获取最后一个波段，无论是否完成
        if last_swing is None:
            logger.error("不应该执行这里！已经有波段了，却获取不到最后一个波段", df_swing=self.swing_manager.df_swing)
            raise AssertionError("不应该执行这里！已经有波段了，却获取不到最后一个波段")
        prev_swing = (
            self.swing_manager.prev_swing(last_swing.id) if last_swing else None
        )  # 获取最后一个波段的前一个波段，无论是否完成

        fractal_type = curr_fractal.fractal_type()
        if fractal_type == FractalType.NONE:
            # 不是分型，形成不了波段点，但有可能打破之前的波段，使之前的波段重新延续，所以需要判断是否打破之前的波段
            is_extend = self._check_extend_swing(last_bar, prev_swing) if prev_swing else False
            if is_extend and prev_swing:
                logger.debug(
                    "打破之前的波段，使之前的波段重新延续",
                    last_bar=last_bar,
                    last_swing=last_swing,
                    prev_swing=prev_swing,
                )
                # 打破之前的波段，使之前的波段重新延续
                # 1. 删除last_swing
                # 2. 更新prev_swing的end_id和价格、状态及其相关信息，使其重新延续到最新
                # 3. 更新backtrack_id，使得从prev_swing开始的后续波段都要被重新计算
                prev_swing.state = SwingState.Extending
                prev_swing.cbar_end_id = last_bar.id
                prev_swing.sbar_end_id = last_bar.sbar_end_id
                prev_swing.high_price = max(prev_swing.high_price, last_bar.high_price)
                prev_swing.low_price = min(prev_swing.low_price, last_bar.low_price)
                sbar_stat = self.get_sbar_stat(prev_swing.sbar_start_id, prev_swing.sbar_end_id)  # 更新sbar相关统计信息
                if sbar_stat:
                    prev_swing.volume = sbar_stat["volume"]
                    prev_swing.span = sbar_stat["span"]
                    prev_swing.start_oi = sbar_stat["start_oi"]
                    prev_swing.end_oi = sbar_stat["end_oi"]
                self._del_last_swing()
                self._update_last_swing(prev_swing)
            else:
                logger.debug(
                    "没有打破之前的波段，继续",
                    last_bar=last_bar,
                    last_swing=last_swing,
                    prev_swing=prev_swing,
                )
            return
        if fractal_type == FractalType.BOTTOM:
            if last_swing.direction == Direction.UP:
                

        last_completed_swing = self.swing_manager.get_swing(is_completed=True)

        # 在当前波段未终结前，任何一个时刻都有可能打破前完成波段，使其重新延续
        if last_completed_swing:
            if last_completed_swing.direction == Direction.DOWN:
                if last_bar.low_price < last_completed_swing.low_price:
                    # 新bar比下降波段的最低价还低，重新延续波段
                    last_completed_swing.cbar_end_id = last_bar.id
                    last_completed_swing.low_price = last_bar.low_price
                    last_completed_swing.sbar_end_id = last_bar.sbar_end_id
                    last_completed_swing.is_completed = False

                    self._del_last_swing()
                    self._update_last_swing(last_completed_swing)
                    logger.debug(
                        "打开前一完成波段，延续",
                        last_completed_swing=last_completed_swing,
                        last_bar=last_bar,
                    )
                    return
            elif last_completed_swing.direction == Direction.UP:
                if last_bar.high_price > last_completed_swing.high_price:
                    last_completed_swing.cbar_end_id = last_bar.id
                    last_completed_swing.high_price = last_bar.high_price
                    last_completed_swing.sbar_end_id = last_bar.sbar_end_id

                    last_completed_swing.is_completed = False

                    self._del_last_swing()
                    self._update_last_swing(last_completed_swing)
                    logger.debug(
                        "打开前一完成波段，延续",
                        last_completed_swing=last_completed_swing,
                        last_bar=last_bar,
                    )
                    return
            else:
                logger.error(
                    "不应该执行这里！波段方向取值不符合要求",
                    last_completed_swing=last_completed_swing,
                )
                raise AssertionError("不应该执行这里！波段方向取值不符合要求")

        active_swing = self.swing_manager.get_last_swing()
        if active_swing is None:
            return
        # 更新 active swing 价格
        active_swing.high_price = max(active_swing.high_price, curr_fractal.high_price)
        active_swing.low_price = min(active_swing.low_price, curr_fractal.low_price)

        if self.determine_swing(
            start_fractal=self.swing_manager.cbar_manager.get_fractal(active_swing.cbar_start_id),
            end_fractal=curr_fractal,
            direction=active_swing.direction,
            curr_swing=active_swing,
            prev_swing=last_completed_swing,
        ):  # 两个分形可以组成一个波段
            active_swing.cbar_end_id = curr_fractal.id  # 波段完成时
            active_swing.is_completed = True

            self._update_last_swing(active_swing)
            logger.debug(
                "完结波段",
                new_swing=active_swing,
            )

            # 说明当前波段已经完成，需要以终止点为新的起点构建active_swing
            # 开始一个新波段
            new_swing = {
                "direction": active_swing.direction.opposite,
                "cbar_start_id": active_swing.cbar_end_id,
                "cbar_end_id": last_bar.id,
                "sbar_start_id": active_swing.sbar_end_id,
                "sbar_end_id": last_bar.sbar_end_id,
                # 此时，波段处于未完成状态，end_id为最新bar的索引，并不是分形顶底bar
                "high_price": (
                    active_swing.high_price if active_swing.direction == Direction.UP else last_bar.high_price
                ),
                "low_price": active_swing.low_price if active_swing.direction == Direction.DOWN else last_bar.low_price,
            }

            # 待确认新波段的起点是否需要调整，如果新波段的起点分形与终点分形之间能构成一个有效波段，则以新波段的起点分形为新的起点，否则保持不变，等待后续分形来确认
            self._append_swing(
                direction=new_swing["direction"],
                state=SwingState.Tentative,
                cbar_start_id=new_swing["cbar_start_id"],
                cbar_end_id=new_swing["cbar_end_id"],
                sbar_start_id=new_swing["sbar_start_id"],
                sbar_end_id=new_swing["sbar_end_id"],
                high_price=new_swing["high_price"],
                low_price=new_swing["low_price"],
            )
            logger.debug(
                "创建新波段",
                new_swing=new_swing,
            )
        else:
            # 不能确认波段终结，即波段延续
            active_swing.cbar_end_id = curr_fractal.cbar_end_id  # 波段未完成，记录最新k
            active_swing.sbar_end_id = self.swing_manager.cbar_manager.get_limit_sbar_id(
                curr_fractal.right.sbar_start_id,
                curr_fractal.right.sbar_end_id,
                "max" if active_swing.direction == Direction.DOWN else "min",
            )
            active_swing.is_completed = False

            self._update_last_swing(active_swing)
            # logger.debug(
            #     "情况3，最后3根bar能组成分形。且与之前波段同向，延续波段。",
            #     active_swing=active_swing,
            # )

    def _build_first_swing(self, curr_fractal: Fractal):
        """
        首次波段创建，需要动态选择起点分型，尽可能早的发现波段
        """
        # 1. 当前active swing是以底分型b0开始的，之后出现的顶分型t0并未满足波段形成的条件，但随后出现的底分型b1的价格已经低于active swing的底分型b0，说明b0并不是一个合格的波段起点，需要：
        # 1.a t0与b1之间可以形成有效的波段，则以t0为新起点，b1为终点，构建波段；
        # 1.b t0与b1之间不能形成有效的波段，则以b1为新起点，构建未完成波段，等待后续分型来确认波段的有效性；并记录最高顶分型max_t=t0
        # 1.c 如果b2与max_t之间能构成波段，则以max_t为起点，b2为终点，构建波段；以此类推，目的是尽快找到第一条波段
        #
        new_swing = self.first_swing_discover.discover(curr_fractal)
        if new_swing is None:
            return
        # 以分形为基础创建波段起点
        logger.debug("首次波段创建", first_swing=new_swing)
        self._append_swing(
            direction=new_swing["direction"],
            state=new_swing["state"],
            cbar_start_id=new_swing["cbar_start_id"],
            cbar_end_id=new_swing["cbar_end_id"],
            sbar_start_id=new_swing["sbar_start_id"],
            sbar_end_id=new_swing["sbar_end_id"],
            high_price=new_swing["high_price"],
            low_price=new_swing["low_price"],
        )

    def _check_extend_swing(self, last_bar: CBar, swing: Swing) -> bool:
        """
        判断当前k线是否打破波段，如果打破了，则之前的波段需要重新延续
        """
        if swing.direction == Direction.UP:
            return last_bar.low_price < swing.low_price
        else:
            return last_bar.high_price > swing.high_price

    def get_sbar_stat(self, sbar_start_id: int, sbar_end_id: int) -> dict | None:
        """
        获取sbar统计信息，包括span、volume、start_oi、end_oi
        """
        stat_rlt = self.swing_manager.cbar_manager.sbar_manager.stat(sbar_start_id, sbar_end_id)
        if stat_rlt:
            return {
                "span": stat_rlt.get("span", 0),
                "volume": stat_rlt.get("volume", 0),
                "start_oi": stat_rlt.get("start_oi", 0),
                "end_oi": stat_rlt.get("end_oi", 0),
            }
        return None

    def _del_last_swing(self) -> None:
        last_swing = self.swing_manager.get_last_swing()
        if last_swing:
            self.swing_manager.backtrack_id = (
                min(last_swing.id, self.swing_manager.backtrack_id)
                if self.swing_manager.backtrack_id
                else last_swing.id
            )
            self.swing_manager.df_swing = self.swing_manager.df_swing.slice(
                0, self.swing_manager.df_swing.height - 1
            )  # 删除未完成的波段

    def _append_swing(
        self,
        direction: Direction,
        state: SwingState,
        cbar_start_id: int,
        cbar_end_id: int,
        sbar_start_id: int,
        sbar_end_id: int,
        high_price: float,
        low_price: float,
    ):
        """
        在df_swing末尾追加一条波段记录，如果state=SwingState.Tentative,则需要以波段结束点为新的起点创建一个state=SwingState.Extending波段
        """
        new_swing = {
            "id": self.swing_manager.id_gen.get_id(),
            "span": 0,
            "volume": 0,
            "start_oi": 0,
            "end_oi": 0,
            "state": state.value,
            "created_at": Datetime.now(),
        }
        new_swing["direction"] = direction.value
        new_swing["cbar_start_id"] = cbar_start_id
        new_swing["cbar_end_id"] = cbar_end_id
        new_swing["sbar_start_id"] = sbar_start_id
        new_swing["sbar_end_id"] = sbar_end_id
        new_swing["high_price"] = high_price
        new_swing["low_price"] = low_price

        # 调整波段终点的位置，确保是在波段区间内的极值位置
        limit_end_cbar = self.swing_manager.cbar_manager.get_limit_cbar(
            cbar_start_id, cbar_end_id, "min" if direction == Direction.DOWN else "max"
        )
        if limit_end_cbar is None:
            logger.error("获取get_limit_cbar失败，请检查cbar_manager中cbar数据的完整性和正确性", swing=new_swing)
            raise AssertionError("获取get_limit_cbar失败，请检查cbar_manager中cbar数据的完整性和正确性")
        new_swing["cbar_end_id"] = limit_end_cbar.id
        new_swing["sbar_end_id"] = self.swing_manager.cbar_manager.get_limit_sbar_id(
            limit_end_cbar.sbar_start_id, limit_end_cbar.sbar_end_id, "min" if direction == Direction.DOWN else "max"
        )
        new_swing["high_price"] = max(new_swing["high_price"], limit_end_cbar.high_price)
        new_swing["low_price"] = min(new_swing["low_price"], limit_end_cbar.low_price)
        if new_swing["sbar_end_id"] is None:
            logger.error("获取get_limit_sbar_id失败，请检查cbar_manager中cbar数据的完整性和正确性", swing=new_swing)
            raise AssertionError("获取get_limit_sbar_id失败，请检查cbar_manager中cbar数据的完整性和正确性")

        # 统计信息
        sbar_stat = self.get_sbar_stat(sbar_start_id, sbar_end_id)
        if sbar_stat is None:
            logger.error("获取sbar_stat失败，请检查cbar_manager中cbar数据的完整性和正确性", swing=new_swing)
            raise AssertionError("获取sbar_stat失败，请检查cbar_manager中cbar数据的完整性和正确性")
        new_swing["span"] = sbar_stat["span"]
        new_swing["volume"] = sbar_stat["volume"]
        new_swing["start_oi"] = sbar_stat["start_oi"]
        new_swing["end_oi"] = sbar_stat["end_oi"]

        data = [new_swing]
        if state != SwingState.Extending:
            last_cbar = self.swing_manager.cbar_manager.get_last_cbar()
            if last_cbar is None:
                logger.error("获取last_cbar失败，请检查cbar_manager中cbar数据的完整性和正确性", swing=new_swing)
                raise AssertionError("获取last_cbar失败，请检查cbar_manager中cbar数据的完整性和正确性")
            # 统计信息
            sbar_stat = self.get_sbar_stat(sbar_start_id, sbar_end_id)
            if sbar_stat is None:
                logger.error("获取sbar_stat失败，请检查cbar_manager中cbar数据的完整性和正确性", swing=new_swing)
                raise AssertionError("获取sbar_stat失败，请检查cbar_manager中cbar数据的完整性和正确性")

            extending_swing = {
                "id": self.swing_manager.id_gen.get_id(),
                "direction": direction.opposite.value,
                "cbar_start_id": limit_end_cbar.id,
                "cbar_end_id": last_cbar.id,
                "sbar_start_id": limit_end_cbar.sbar_start_id,
                "sbar_end_id": last_cbar.sbar_end_id,
                "high_price": max(limit_end_cbar.high_price, last_cbar.high_price),
                "low_price": min(limit_end_cbar.low_price, last_cbar.low_price),
                "span": sbar_stat["span"],
                "volume": sbar_stat["volume"],
                "start_oi": sbar_stat["start_oi"],
                "end_oi": sbar_stat["end_oi"],
                "state": SwingState.Extending.value,
                "created_at": Datetime.now(),
            }
            data.append(extending_swing)

        self.swing_manager.df_swing = self.swing_manager.df_swing.vstack(
            pl.DataFrame(data, schema=self.swing_manager.df_swing.schema)
        )

    def _update_last_swing(self, swing: Swing):
        if self.swing_manager.df_swing.height > 0:
            del_swing_id = self.swing_manager.df_swing.tail(1).select(pl.col("id")).item()
            self.swing_manager.backtrack_id = (
                min(del_swing_id, self.swing_manager.backtrack_id) if self.swing_manager.backtrack_id else del_swing_id
            )  # 记录回溯id
            last_idx = self.swing_manager.df_swing.height - 1
            self.swing_manager.df_swing[last_idx, "id"] = self.swing_manager.id_gen.get_id(),
            self.swing_manager.df_swing[last_idx, "direction"] = swing.direction.value
            self.swing_manager.df_swing[last_idx, "cbar_start_id"] = swing.cbar_start_id
            self.swing_manager.df_swing[last_idx, "cbar_end_id"] = swing.cbar_end_id
            self.swing_manager.df_swing[last_idx, "sbar_start_id"] = swing.sbar_start_id
            self.swing_manager.df_swing[last_idx, "sbar_end_id"] = swing.sbar_end_id
            self.swing_manager.df_swing[last_idx, "high_price"] = swing.high_price
            self.swing_manager.df_swing[last_idx, "low_price"] = swing.low_price
            self.swing_manager.df_swing[last_idx, "span"] = swing.span
            self.swing_manager.df_swing[last_idx, "volume"] = swing.volume
            self.swing_manager.df_swing[last_idx, "start_oi"] = swing.start_oi
            self.swing_manager.df_swing[last_idx, "end_oi"] = swing.end_oi
            self.swing_manager.df_swing[last_idx, "state"] = swing.state.value
            self.swing_manager.df_swing[last_idx, "created_at"] = Datetime.now()

    def determine_swing(
        self,
        start_fractal: Fractal | None,
        end_fractal: Fractal | None,
        direction: Direction,
        curr_swing: Swing | None = None,
        prev_swing: Swing | None = None,
    ) -> bool:
        """
        判定两个分形是否能够组成波段
        """
        if start_fractal is None or end_fractal is None:
            logger.error(
                "determine_swing 在调用determine_swing方法时，参数值有None",
                curr_swing=curr_swing,
                prev_swing=prev_swing,
            )
            raise AssertionError("start_fractal and end_fractal are both None")

        if curr_swing is not None and direction != curr_swing.direction:
            logger.error(
                "determine_swing 在调用determine_swing方法时，curr_swing的方向与传入的direction不一致",
                curr_swing=curr_swing,
                direction=direction,
            )
            raise AssertionError("curr_swing direction is not consistent with the input direction")
        start_fractal_type = start_fractal.fractal_type()
        end_fractal_type = end_fractal.fractal_type()

        if start_fractal_type == FractalType.NONE or end_fractal_type == FractalType.NONE:
            # logger.debug("determine_swing 波段端点并非有效分形")
            return False
        if start_fractal_type == end_fractal_type:  # 同向分形不可能组成波段，必须是不同向分形才行
            # logger.debug("determine_swing 相邻同向分形，不能构成分形")
            return False
        # 对波段顶底分形的位置进行判断，上升波段顶分形要在底分形之上，下降波段底分形要在顶分形之下
        if direction == Direction.UP:
            if end_fractal.high_price < start_fractal.low_price:
                logger.debug(
                    "determine_swing 在上升波段中，终止分形比开始分形还低，不能形成波段",
                    start_fractal=start_fractal,
                    end_fractal=end_fractal,
                    curr_swing=curr_swing,
                )
                return False
        else:
            if end_fractal.low_price > start_fractal.high_price:
                logger.debug(
                    "determine_swing 在下降波段中，终止分形比开始分形还高，不能形成波段",
                    start_fractal=start_fractal,
                    end_fractal=end_fractal,
                    curr_swing=curr_swing,
                )
                return False

        if not start_fractal.overlap(end_fractal):  # 两个分形没有重叠可形成波段
            logger.debug(
                "determine_swing 两个分形没有重叠可形成波段",
                start_fractal=start_fractal,
                end_fractal=end_fractal,
                curr_swing=curr_swing,
                prev_swing=prev_swing,
            )
            return True
        # <<可以添加波动率为准绳的条件>>
        # 如果分形之间有重叠，
        # 1）但两分形中间cbar并未重叠，
        # 2）且已经超过了前一波段的60%，
        # 3）并且在包含合并处理之前的sbar中，间隔超过5根K线，
        # 那么，也视为波段成立（因为反抗力量确实也足够）
        if prev_swing is None:
            logger.debug("determine_swing prev_swing为None，波动率比较取消")
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

    def _clean_backtrack(self, cbar_backtrack_id: int):
        df = self.swing_manager.df_swing.filter(
            (pl.col("cbar_start_id") <= cbar_backtrack_id) & (cbar_backtrack_id <= pl.col("cbar_end_id"))
        )
        if df.is_empty():
            return

        del_swing = Swing(**df.row(0, named=True))
        del_swing_idx = self.swing_manager.get_index(del_swing.id)

        self.swing_manager.df_swing = self.swing_manager.df_swing.slice(
            0, del_swing_idx
        )  # 删除从第一个traceback_id出现时的波段
        # TODO 更新最后一条记录的state

        # 取出删除之前最后处理的cbar,填补swing信息
        end_bar = self.swing_manager.cbar_manager.get_nearest_cbars(cbar_backtrack_id, -1)
        logger.debug("swing中需删除backtrack_id后，重新开始的位置", end_bar=end_bar)
        if end_bar:  # 如果end_bar为None，说明df_cbar在traceback_id之前已没有数据，重新构建波段
            # 不为None，修改del_swing并重新添加到df_swing
            end_bar = end_bar[0]
            if del_swing.cbar_start_id == cbar_backtrack_id:  # 在波段起点
                del_swing.cbar_start_id = end_bar.id
                del_swing.sbar_start_id = end_bar.sbar_start_id

            del_swing.cbar_end_id = end_bar.id
            del_swing.sbar_end_id = end_bar.sbar_end_id
            del_swing.high_price = max(del_swing.high_price, end_bar.high_price)
            del_swing.low_price = min(del_swing.low_price, end_bar.low_price)
            del_swing.is_completed = False
            self._append_swing(del_swing)

        self.swing_manager.backtrack_id = del_swing.id

    def _backtrack_replay(self, backtrack_id: int):
        """
        查找要从哪个cbar开始回放处理，取值min(backtrack_id, swing.end_id)
        """
        # 2. 获取需要处理的cbar[backtrack_id, last_id]，进行回放
        cbar_list = self.swing_manager.cbar_manager.get_nearest_cbars(backtrack_id)
        if cbar_list is None:
            return
        for cbar in cbar_list:
            self._build_swing(cbar)
