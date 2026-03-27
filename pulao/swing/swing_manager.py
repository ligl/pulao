from typing import Any, List, Literal

import polars as pl
from datetime import datetime

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
        self.backtrack_swing_id: int | None = (
            None  # swing变动之后，告诉订阅者，从哪个swing id开始重新计算，大于等于此id的都要被重新计算
        )
        self.backtrack_cbar_id: int | None = (
            None  # cbar变动之后，告诉订阅者，从哪个cbar id开始重新计算，大于等于此id的都要被重新计算
        )
        self.symbol = self.cbar_manager.symbol
        self.timeframe = self.cbar_manager.timeframe
        self.swing_builder = _SwingBuilder(self)

    def _on_cbar_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        self.backtrack_swing_id = None
        self.backtrack_cbar_id = payload.get("backtrack_id", None)
        # 波段检测识别

        # logger.debug("_on_cbar_created", payload=payload)
        if self.backtrack_cbar_id is None:
            self.swing_builder._build_swing()
        else:
            self.swing_builder._clean_backtrack(self.backtrack_cbar_id)
            self.swing_builder._backtrack_replay(self.backtrack_cbar_id)
        self.write_parquet()
        self.notify(timeframe, EventType.SWING_CHANGED, backtrack_id=self.backtrack_swing_id)

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

    def get_swing_fractal(self, swing_id: int | Swing) -> tuple[Fractal | None, Fractal | None]:
        if isinstance(swing_id, Swing):
            swing = swing_id
        else:
            swing = self.get_swing(swing_id)
        if swing is None:
            return None, None
        start_fractal = self.cbar_manager.get_fractal(swing.cbar_start_id)
        end_fractal = self.cbar_manager.get_fractal(swing.cbar_end_id)
        return start_fractal, end_fractal

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
        start_idx = self.get_idx(start_id)
        end_idx = self.get_idx(end_id)
        if start_idx is None or end_idx is None:
            return None
        if start_idx > end_idx:  # 交换
            start_idx, end_idx = end_idx, start_idx

        df = self.df_swing.slice(start_idx, end_idx - start_idx + 1).filter(pl.col("direction") == direction)
        if df.is_empty():
            return None
        if arg == "max":
            idx = df["high_price"].arg_max()
        else:
            idx = df["low_price"].arg_min()
        return Swing(**df.row(idx, named=True))

    def get_limit_swing_id(
        self, start_id: int, end_id: int, arg: Literal["max", "min"], direction: Direction
    ) -> int | None:
        swing = self.get_limit_swing(start_id, end_id, arg, direction)
        if swing is None:
            return None
        return swing.id

    def get_idx(self, id: int) -> int | None:
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
            end_idx = idx - 1
            start_idx = end_idx - count + 1
        else:  # 向后
            start_idx = idx + 1
            end_idx = start_idx + count - 1

        if start_idx < 0:
            start_idx = 0
            end_idx = idx - 1
        if end_idx < 0:
            return None

        df = self.df_swing.slice(start_idx, end_idx - start_idx + 1)
        if df.is_empty():
            return None

        if is_return_single:
            return Swing(**df.row(0, named=True))

        return [Swing(**row) for row in df.rows(named=True)]

    def get_last_swing(self) -> None | Swing:

        if self.df_swing.height == 0:
            return None
        df = self.df_swing.tail(1)
        return Swing(**df.row(0, named=True))

    def get_swing(self, id: int | None = None, is_completed: bool | None = None) -> Swing | None:
        """
        获取指定波段
        :param id: 指定id的波段，如果没有指定，获取最新波段
        :param is_completed: None：不限制
        :return: Swing | None
        """
        if id is None:
            idx = self.df_swing.height - 1  # 取最后一条
        else:
            idx = self.get_idx(id)

        if idx is None or idx < 0 or idx > self.df_swing.height - 1:
            return None

        swing = Swing(**self.df_swing.row(idx, named=True))
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

    def get_swing_by_idx(self, idx: int) -> Swing | None:
        if idx is None or idx < 0 or idx > self.df_swing.height - 1:
            return None
        return Swing(**self.df_swing.row(idx, named=True))

    def prev_opposite_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        idx = self.get_idx(id)
        if idx is None:
            return None
        return self.get_swing_by_idx(idx - 1)

    def prev_same_swing(self, id: int) -> Swing | None:
        """
        前一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        idx = self.get_idx(id)
        if idx is None:
            return None
        return self.get_swing_by_idx(idx - 2)

    def next_opposite_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相反方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        idx = self.get_idx(id)
        if idx is None:
            return None
        return self.get_swing_by_idx(idx + 1)

    def next_same_swing(self, id: int) -> Swing | None:
        """
        后一个与指定波段相同方向的波段
        :param id: 指定id所在的波段
        :return: Swing | None
        """
        idx = self.get_idx(id)
        if idx is None:
            return None
        return self.get_swing_by_idx(idx + 2)

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
        start_idx = self.get_idx(start_id)
        end_idx = self.get_idx(end_id)
        if start_idx is None or end_idx is None:
            return None
        if start_idx > end_idx:  # 交换
            start_idx, end_idx = end_idx, start_idx

        df = self.df_swing.slice(start_idx, end_idx - start_idx + 1)
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


class _SwingDiscoverer:
    """
    波段发现器，负责根据k线数据发现波段
    """

    __slots__ = ["swing_builder", "bottom_fractal", "top_fractal"]

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
        # logger.debug("discover - entry", curr_fractal=fractal, bottom_fractal=self.bottom_fractal, top_fractal=self.top_fractal)
        self.reset_by_backtrack()
        # 找分型的过程中，更新当前的顶底分形，如果出现了新的分形，说明之前的顶底分形已经不能构成波段了，需要更新状态
        fractal_type = fractal.fractal_type()
        match fractal_type:
            case FractalType.BOTTOM:
                if self.bottom_fractal is None:
                    self.bottom_fractal = fractal
                elif fractal.low_price <= self.bottom_fractal.low_price:
                    # 出现了更低的底分形，更新底分形
                    self.bottom_fractal = fractal
            case FractalType.TOP:
                if self.top_fractal is None:
                    self.top_fractal = fractal
                elif fractal.high_price >= self.top_fractal.high_price:
                    # 出现了更高的顶分形，更新顶分形
                    self.top_fractal = fractal
            case _:
                # 波段发现器只处理分型，不是分形的，不符合条件，直接丢弃
                logger.debug("discover分形类型为None不符合条件，丢弃", drop_fractal=fractal)
                return
        # 已经同时有了顶分形和底分形，说明在等待新的分形来确认波段
        if self.bottom_fractal and self.top_fractal:
            logger.debug(
                "discover", curr_fractal=fractal, bottom_fractal=self.bottom_fractal, top_fractal=self.top_fractal
            )

            # 检测当前顶底分型是否能构成一个波段，如果能构成一个波段，则返回新波段信息，并重置状态
            if self.bottom_fractal.id < self.top_fractal.id:  # 按照时间顺序确定波段方向
                direction = Direction.UP
                start_fractal = self.bottom_fractal
                end_fractal = self.top_fractal
            else:
                direction = Direction.DOWN
                start_fractal = self.top_fractal
                end_fractal = self.bottom_fractal

            is_swing = self.swing_builder.detect_swing(
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
                # clear state
                self.bottom_fractal = None
                self.top_fractal = None

                return new_swing

    def reset_by_backtrack(self):
        # 如果有backtrack_id，说明是回溯重放过程中发现的分形，此时需要判断这个分形是否在当前顶底分形的回溯范围内，
        # 如果不在回溯范围内，说明这个分形是新的，不受回溯影响，需要被发现器处理；
        # 如果在回溯范围内，说明这个分形是旧的，已经被发现器处理过了，需要被丢弃，重新计算
        bactrack_cbar_id = self.swing_builder.swing_manager.backtrack_cbar_id
        if bactrack_cbar_id is not None:
            if self.top_fractal is not None:
                if self.top_fractal.cbar_start_id <= bactrack_cbar_id <= self.top_fractal.cbar_end_id:
                    logger.debug(
                        "discover在回溯范围内，说明这个分形是旧的，丢弃",
                        backtrack_cbar_id=bactrack_cbar_id,
                        top_fractal=self.top_fractal,
                    )
                    self.top_fractal = None
            if self.bottom_fractal is not None:
                if self.bottom_fractal.cbar_start_id <= bactrack_cbar_id <= self.bottom_fractal.cbar_end_id:
                    logger.debug(
                        "discover在回溯范围内，说明这个分形是旧的，丢弃",
                        backtrack_cbar_id=bactrack_cbar_id,
                        bottom_fractal=self.bottom_fractal,
                    )
                    self.bottom_fractal = None


class _SwingBuilder:
    """
    波段构建器，负责根据k线数据构建波段
    """

    __slots__ = ["swing_manager", "first_swing_discover"]

    def __init__(self, swing_manager: SwingManager):
        self.swing_manager = swing_manager
        self.first_swing_discover = _SwingDiscoverer(self)  # 保存首次构建波段的探索状态

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

        left_bar, middle_bar, last_bar = cbar_list

        curr_fractal = Fractal(left=left_bar, middle=middle_bar, right=last_bar)

        # 首次波段创建
        if self.swing_manager.df_swing.is_empty():
            return self._build_first_swing(curr_fractal)

        # 已有波段，判断是延续、候选待确认还是确认终结波段
        last_swing = self.swing_manager.get_last_swing()  # 获取最后一个波段，无论是否完成
        if last_swing is None:
            logger.error("不应该执行这里！已经有波段了，却获取不到最后一个波段", df_swing=self.swing_manager.df_swing)
            raise AssertionError("不应该执行这里！已经有波段了，却获取不到最后一个波段")
        prev_swing = self.swing_manager.prev_swing(last_swing.id)  # 获取最后一个波段的前一个波段，无论是否完成

        fractal_type = curr_fractal.fractal_type()
        match fractal_type:
            case FractalType.NONE:
                # 不是分型，形成不了波段点，但有可能打破之前的波段，使之前的波段重新延续，所以需要判断是否打破之前的波段
                is_break = self._check_reverse_break_swing(last_bar, last_swing)
                if is_break:
                    # 打破当前的波段，使之前的波段重新延续
                    logger.debug(
                        "打破当前的波段，使之前的波段重新延续",
                        last_bar=last_bar,
                        last_swing=last_swing,
                        prev_swing=prev_swing,
                    )
                    # 打破之前的波段，使之前的波段重新延续
                    # 1. 删除last_swing
                    # 2. 更新prev_swing的end_id和价格、状态及其相关信息，使其重新延续到最新
                    # 3. 更新backtrack_id，使得从prev_swing开始的后续波段都要被重新计算
                    self._del_swing()
                    if prev_swing is not None:
                        prev_swing.state = SwingState.Extending
                        prev_swing.cbar_end_id = last_bar.id
                        prev_swing.sbar_end_id = last_bar.sbar_end_id
                        prev_swing.high_price = max(prev_swing.high_price, last_bar.high_price)
                        prev_swing.low_price = min(prev_swing.low_price, last_bar.low_price)

                        self._update_swing(prev_swing)
                else:
                    # 没有打破之前的波段，继续延续当前波段
                    logger.debug(
                        "没有打破之前的波段，继续延续当前波段",
                        last_bar=last_bar,
                        last_swing=last_swing,
                        prev_swing=prev_swing,
                    )
                    # 更新last_swing的end_id和价格，使其延续到最新，等待后续分形来确认波段是否终结
                    last_swing.cbar_end_id = last_bar.id
                    last_swing.sbar_end_id = last_bar.sbar_end_id
                    last_swing.high_price = max(last_swing.high_price, last_bar.high_price)
                    last_swing.low_price = min(last_swing.low_price, last_bar.low_price)

                    self._update_swing(last_swing)

            case FractalType.BOTTOM | FractalType.TOP:
                # 上升波段/下降波段 + 底分型，判断分型和当前波段结合是否可以构成有效的波段：
                # 1. 如果能构成有效的波段，说明当前波段终结，构建新波段：
                # 1.1 当前波段进入Tentative状态，等待后续波段确认;
                # 1.2 同时把prev_swing的状态更新为Confirmed,因为prev_swing的状态一直是Confirmed或者Extending，只有当前波段是Tentative，所以当当前波段被确认了，就说明prev_swing也被确认了
                # 2. 如果不能构成有效的波段，说明：
                # 2.1. 当前波段可能延续，等待后续分形确认;
                # 2.2 当前波段的起点可能需要调整（新的底分型比波段起点底分型还低），更当前波段last_swing的起点，并更新前一波段prev_swing的终点
                is_valid_swing = self.detect_swing(
                    start_fractal=self.swing_manager.get_swing_fractal(last_swing)[0],  # 波段起点分型
                    end_fractal=curr_fractal,
                    direction=last_swing.direction,
                    curr_swing=last_swing,
                    prev_swing=prev_swing,
                )
                if is_valid_swing:
                    # 构成了一个有效的波段，说明上一个波段被确认了，当前波段进入Tentative状态
                    logger.debug(
                        "构成了一个有效的下降波段，说明上一个波段被确认了，当前波段进入Tentative状态",
                        last_bar=last_bar,
                        last_swing=last_swing,
                        prev_swing=prev_swing,
                        curr_fractal=curr_fractal,
                    )
                    last_swing.state = SwingState.Tentative
                    last_swing.cbar_end_id = curr_fractal.id
                    last_swing.sbar_end_id = curr_fractal.sbar_end_id
                    last_swing.high_price = max(last_swing.high_price, curr_fractal.high_price)
                    last_swing.low_price = min(last_swing.low_price, curr_fractal.low_price)

                    self._update_swing(last_swing)

                    if prev_swing and prev_swing.state != SwingState.Confirmed:
                        prev_swing.state = SwingState.Confirmed
                        self._update_swing(prev_swing)

                    # 构建新波段
                    new_swing = {
                        "direction": last_swing.direction.opposite,
                        "cbar_start_id": curr_fractal.id,
                        "cbar_end_id": curr_fractal.cbar_end_id,
                        "high_price": curr_fractal.high_price,
                        "low_price": curr_fractal.low_price,
                        "sbar_start_id": curr_fractal.sbar_start_id,
                        "sbar_end_id": curr_fractal.sbar_end_id,
                        "state": SwingState.Extending,
                    }
                    logger.debug("构建新波段", new_swing=new_swing)
                    self._append_swing(**new_swing)
                else:
                    logger.debug(
                        "没有构成一个有效的波段，说明当前波段可能延续，等待后续分形确认",
                        last_bar=last_bar,
                        last_swing=last_swing,
                        prev_swing=prev_swing,
                        curr_fractal=curr_fractal,
                    )
                    last_swing.cbar_end_id = curr_fractal.cbar_end_id
                    last_swing.sbar_end_id = curr_fractal.sbar_end_id
                    last_swing.high_price = max(last_swing.high_price, curr_fractal.high_price)
                    last_swing.low_price = min(last_swing.low_price, curr_fractal.low_price)

                    self._update_swing(last_swing)

    def _build_first_swing(self, fractal: Fractal):
        """
        首次波段创建，需要动态选择起点分型，尽可能早的发现波段
        """
        # 1. 当前active swing是以底分型b0开始的，之后出现的顶分型t0并未满足波段形成的条件，但随后出现的底分型b1的价格已经低于active swing的底分型b0，说明b0并不是一个合格的波段起点，需要：
        # 1.a t0与b1之间可以形成有效的波段，则以t0为新起点，b1为终点，构建波段；
        # 1.b t0与b1之间不能形成有效的波段，则以b1为新起点，构建未完成波段，等待后续分型来确认波段的有效性；并记录最高顶分型max_t=t0
        # 1.c 如果b2与max_t之间能构成波段，则以max_t为起点，b2为终点，构建波段；以此类推，目的是尽快找到第一条波段
        #
        if new_swing := self.first_swing_discover.discover(fractal):
            # 以分形为基础创建波段起点
            logger.debug("首次创建波段", first_swing=new_swing)
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

    def _check_reverse_break_swing(self, cbar: CBar, swing: Swing) -> bool:
        """
        判断当前k线是否打破波段，如果打破了，则之前的波段需要重新延续, 这里的打破是指价格已经反向超过了波段的起点，形成了一个新的极值，说明之前的波段已经被打破了，不再成立了
        """
        if swing.direction == Direction.UP:
            return cbar.low_price < swing.low_price
        else:
            return cbar.high_price > swing.high_price

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

    def _del_swing(self, del_from_id: int | None = None):
        """
        删除波段，如果指定了del_from_id，则删除del_from_id及其之后的波段，否则删除最后一个波段
        @param del_from_id: 从哪个波段开始删除，如果没有指定，则删除最后一个波段
        """
        if del_from_id is None:
            del_from_swing = self.swing_manager.get_last_swing()
        else:
            del_from_swing = self.swing_manager.get_swing(del_from_id)

        if del_from_swing:
            before_del_height = self.swing_manager.df_swing.height
            self.swing_manager.backtrack_swing_id = (
                min(del_from_swing.id, self.swing_manager.backtrack_swing_id)
                if self.swing_manager.backtrack_swing_id
                else del_from_swing.id
            )
            del_from_swing_idx = self.swing_manager.get_idx(del_from_swing.id)
            self.swing_manager.df_swing = self.swing_manager.df_swing.slice(
                0, del_from_swing_idx
            )  # 删除from_swing_id及其之后的波段
            after_del_height = self.swing_manager.df_swing.height
            logger.debug(
                "删除波段",
                del_from_swing_id=del_from_swing.id,
                before_del_height=before_del_height,
                after_del_height=after_del_height,
            )

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
        在df_swing末尾追加一条波段记录，如果添加的波段状态不是Extending，则以波段结束点为新的起点创建一个state=SwingState.Extending反向波段
        """
        new_swing = {
            "id": self.swing_manager.id_gen.get_id(),
            "span": 0,
            "volume": 0,
            "start_oi": 0,
            "end_oi": 0,
            "state": state.value,
            "created_at": datetime.now(),
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

        logger.info(
            "创建新波段",
            new_swing=new_swing,
        )
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

            attach_swing = {
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
                "created_at": datetime.now(),
            }
            logger.info(
                "新创建的波段状态不是Extending，说明之前的波段被打破了，需要创建一个反向的Extending波段",
                source_swing=new_swing,
                attach_swing=attach_swing,
            )
            data.append(attach_swing)

        self.swing_manager.df_swing = self.swing_manager.df_swing.vstack(
            pl.DataFrame(data, schema=self.swing_manager.df_swing.schema)
        )

    def _update_swing(self, swing: Swing):
        """
        更新波段信息
        :param swing: 要更新的波段信息依据, swing.id来定位要更新的波段位置
        """
        idx = self.swing_manager.get_idx(swing.id)
        if idx is None:
            logger.error("未找到要更新的波段在df_swing中的位置", swing=swing)
            raise AssertionError("未找到要更新的波段在df_swing中的位置")
        self.swing_manager.backtrack_swing_id = (
            min(swing.id, self.swing_manager.backtrack_swing_id) if self.swing_manager.backtrack_swing_id else swing.id
        )  # 记录回溯id
        sbar_stat = self.get_sbar_stat(swing.sbar_start_id, swing.sbar_end_id)  # 更新sbar相关统计信息
        if sbar_stat:
            swing.volume = sbar_stat["volume"]
            swing.span = sbar_stat["span"]
            swing.start_oi = sbar_stat["start_oi"]
            swing.end_oi = sbar_stat["end_oi"]

        condition = pl.col("id") == swing.id
        self.swing_manager.df_swing = self.swing_manager.df_swing.with_columns(
            [
                pl.when(condition).then(swing.direction.value).otherwise(pl.col("direction")).alias("direction"),
                pl.when(condition).then(swing.cbar_start_id).otherwise(pl.col("cbar_start_id")).alias("cbar_start_id"),
                pl.when(condition).then(swing.cbar_end_id).otherwise(pl.col("cbar_end_id")).alias("cbar_end_id"),
                pl.when(condition).then(swing.sbar_start_id).otherwise(pl.col("sbar_start_id")).alias("sbar_start_id"),
                pl.when(condition).then(swing.sbar_end_id).otherwise(pl.col("sbar_end_id")).alias("sbar_end_id"),
                pl.when(condition).then(swing.high_price).otherwise(pl.col("high_price")).alias("high_price"),
                pl.when(condition).then(swing.low_price).otherwise(pl.col("low_price")).alias("low_price"),
                pl.when(condition).then(swing.span).otherwise(pl.col("span")).alias("span"),
                pl.when(condition).then(swing.volume).otherwise(pl.col("volume")).alias("volume"),
                pl.when(condition).then(swing.start_oi).otherwise(pl.col("start_oi")).alias("start_oi"),
                pl.when(condition).then(swing.end_oi).otherwise(pl.col("end_oi")).alias("end_oi"),
                pl.when(condition).then(swing.state.value).otherwise(pl.col("state")).alias("state"),
                pl.when(condition).then(datetime.now()).otherwise(pl.col("created_at")).alias("created_at"),
            ]
        )

    def detect_swing(
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

        if curr_swing and direction != curr_swing.direction:
            logger.error(
                "determine_swing 在调用determine_swing方法时，curr_swing的方向与传入的direction不一致",
                curr_swing=curr_swing,
                input_direction=direction,
            )
            raise AssertionError("curr_swing direction is not consistent with the input direction")
        start_fractal_type = start_fractal.fractal_type()
        end_fractal_type = end_fractal.fractal_type()

        if start_fractal_type == FractalType.NONE or end_fractal_type == FractalType.NONE:
            logger.debug("determine_swing 波段端点并非有效分形", start_fractal=start_fractal, end_fractal=end_fractal)
            return False
        if start_fractal_type == end_fractal_type:  # 同向分形不可能组成波段，必须是不同向分形才行
            logger.debug(
                "determine_swing 相邻同向分形，不能构成分形", start_fractal=start_fractal, end_fractal=end_fractal
            )
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
            logger.debug("determine_swing 开启分型非严格比较，两个分型中间k线有重叠，不可以形成波段")
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

    def _clean_backtrack(self, backtrack_cbar_id: int):
        df = self.swing_manager.df_swing.filter(
            (pl.col("cbar_start_id") <= backtrack_cbar_id) & (backtrack_cbar_id <= pl.col("cbar_end_id"))
        )
        if df.is_empty():
            return

        del_from_swing = Swing(**df.row(0, named=True))

        self._del_swing(del_from_swing.id)  # 删除del_from_swing及其之后的波段

        prev_cbars = self.swing_manager.cbar_manager.get_nearest_cbars(
            backtrack_cbar_id, -1
        )  # backtrack_cbar_id之前的最后一个cbar
        prev_cbar = prev_cbars[0] if prev_cbars else None
        logger.debug("_clean_backtrack swing中需删除cbar.backtrack_id后，cbar重新开始的位置", prev_cbar=prev_cbar)
        if prev_cbar is None:
            return
        if del_from_swing.cbar_start_id == backtrack_cbar_id:
            # 如果backtrack_cbar_id正好是波段的起点，则更新前一波段的终点为prev_cbar，并把状态改为Extending
            prev_swing = self.swing_manager.prev_swing(del_from_swing.id)
            logger.debug(
                "_clean_backtrack backtrack_cbar_id正好是波段的起点，删除波段并更新前一波段状态",
                prev_swing=prev_swing,
                del_from_swing=del_from_swing,
            )
            if prev_swing:
                prev_swing.cbar_end_id = prev_cbar.id
                prev_swing.sbar_end_id = prev_cbar.sbar_end_id
                prev_swing.high_price = max(prev_swing.high_price, prev_cbar.high_price)
                prev_swing.low_price = min(prev_swing.low_price, prev_cbar.low_price)
                prev_swing.state = SwingState.Extending

                self._update_swing(prev_swing)
        else:
            # 否则，说明backtrack_cbar_id在波段区间内，把波段的终点改为prev_cbar，并把状态改为Extending
            logger.debug(
                "_clean_backtrack backtrack_cbar_id在波段区间内，更新波段终点并改为Extending",
                del_from_swing=del_from_swing,
            )
            self._append_swing(
                direction=del_from_swing.direction,
                state=SwingState.Extending,
                cbar_start_id=del_from_swing.cbar_start_id,
                cbar_end_id=prev_cbar.id,
                sbar_start_id=del_from_swing.sbar_start_id,
                sbar_end_id=prev_cbar.sbar_end_id,
                high_price=max(del_from_swing.high_price, prev_cbar.high_price),
                low_price=min(del_from_swing.low_price, prev_cbar.low_price),
            )

    def _backtrack_replay(self, backtrack_cbar_id: int):
        """
        查找要从哪个cbar开始回放处理，取值min(backtrack_id, swing.end_id)
        """
        # 2. 获取需要处理的cbar[backtrack_id, last_id]，进行回放
        cbar_list = self.swing_manager.cbar_manager.get_nearest_cbars(backtrack_cbar_id)
        if cbar_list is None:
            return
        for cbar in cbar_list:
            self._build_swing(cbar)
