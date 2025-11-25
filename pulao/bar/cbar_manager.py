from __future__ import annotations

from typing import Any, List

from pulao.constant import (
    EventType,
    SwingDirection, FractalType,
)
from pulao.events import Observable
from pulao.bar import SBar, SBarManager, CBar, Fractal

import polars as pl


class CBarManager(Observable):
    def __init__(self, sbar_manager: SBarManager):
        super().__init__()
        schema = {
            "index": pl.UInt32,
            "start_index": pl.UInt32,
            "end_index": pl.UInt32,
            "high_price": pl.Float32,
            "low_price": pl.Float32,
            "fractal_type": pl.Int8,
        }
        self.df_cbar: pl.DataFrame = pl.DataFrame(schema=schema)  # 包含合并后的k线列表
        self.sbar_manager: SBarManager = sbar_manager
        self.sbar_manager.subscribe(self._on_sbar_created)

    def _on_sbar_created(self, event: EventType, sbar: Any):
        # 1. K线包含处理
        self._agg_bar(sbar)
        # 2. 分形检测
        self._detect_fractal()

        self.notify(EventType.CBAR_CREATED)

    def _agg_bar(self, sbar: SBar):
        """
        K线包含关系处理（缠论预处理第一步）
        将原始K线合并为无包含关系的处理后K线序列
        """
        # 当前待处理的原始K线
        curr_high = sbar.high_price
        curr_low = sbar.low_price
        curr_idx = sbar.index

        # 用于构建新合并K线的临时变量
        merged_high = curr_high
        merged_low = curr_low
        start_idx = curr_idx
        end_idx = curr_idx

        # 情况1：df_cbar 为空 → 直接加入第一根
        if self.df_cbar.is_empty():
            self._append_cbar(start_idx, end_idx, merged_high, merged_low)
            return

        # 情况2：已有数据，取最后一根处理后的K线
        last_cbar_dict = self.df_cbar.tail(1).row(0, named=True)
        last_cbar = CBar(**last_cbar_dict)

        last_high = last_cbar.high_price
        last_low = last_cbar.low_price

        # 判断两根K线是否存在包含关系
        def is_inclusive(a_high, a_low, b_high, b_low):
            return (a_high >= b_high and a_low <= b_low) or (
                a_high <= b_high and a_low >= b_low
            )

        # 判断趋势方向（通过最后一根处理后K线与再往前一根比较）
        direction = None
        if self.df_cbar.height >= 2:
            prev_dict = self.df_cbar.tail(2).row(0, named=True)
            prev_cbar = CBar(**prev_dict)
            if last_high > prev_cbar.high_price:
                direction = SwingDirection.UP
            elif last_low < prev_cbar.low_price:
                direction = SwingDirection.DOWN
            # else: 第一个合并段，还没有明确方向，后面会处理

        # 如果没有明确方向（只有1根），则按“先高后低”或“先低后高”定方向（常见做法）
        if direction is None:
            if curr_high >= curr_low:  # 正常情况
                if last_high >= last_low:
                    # 都阳线或十字，按收盘或最高最低定，简单处理：谁高谁定向上
                    direction = (
                        SwingDirection.UP
                        if curr_high >= last_high
                        else SwingDirection.DOWN
                    )
                else:
                    direction = SwingDirection.UP
            else:
                direction = SwingDirection.DOWN

        # 开始包含处理
        included = is_inclusive(last_high, last_low, curr_high, curr_low)

        if included:
            # 有包含关系 → 合并，且按已有趋势方向处理高低点
            start_idx = last_cbar.start_index

            if direction == SwingDirection.UP:
                merged_high = max(last_high, curr_high)
                merged_low = max(last_low, curr_low)  # 向上趋势，低点取较高的
            else:  # DOWN
                merged_high = min(last_high, curr_high)  # 向下趋势，高点取较低的
                merged_low = min(last_low, curr_low)

            # 移除最后一条（因为要被合并替换）
            self.df_cbar = self.df_cbar.slice(0, self.df_cbar.height - 1)

            # 关键：可能还需要向前继续合并！
            # 例如：1→2（包含）→3（又被1包含），必须一直向前吃
            while self.df_cbar.height >= 2:
                # 取新的最后两根
                new_last = CBar(**self.df_cbar.tail(1).row(0, named=True))
                prev = CBar(**self.df_cbar.tail(2).row(0, named=True))

                if direction == SwingDirection.UP:
                    if new_last.high_price <= prev.high_price:
                        break  # 已经破坏向上趋势，停止向前合并
                else:
                    if new_last.low_price >= prev.low_price:
                        break  # 破坏向下趋势

                # 检查新last是否还被之前的包含
                if is_inclusive(
                    prev.high_price,
                    prev.low_price,
                    new_last.high_price,
                    new_last.low_price,
                ):
                    # 继续合并
                    start_idx = prev.start_index
                    if direction == SwingDirection.UP:
                        merged_high = max(merged_high, prev.high_price)
                        merged_low = max(merged_low, prev.low_price)
                    else:
                        merged_high = min(merged_high, prev.high_price)
                        merged_low = min(merged_low, prev.low_price)
                    # 删除倒数第二根（现在成了最后）
                    self.df_cbar = self.df_cbar.slice(0, self.df_cbar.height - 1)
                else:
                    break
        else:
            # 无包含关系 → 直接作为新K线加入
            merged_high = curr_high
            merged_low = curr_low
            start_idx = curr_idx

        # 最终追加合并后的K线
        self._append_cbar(start_idx, end_idx, merged_high, merged_low)

    def _append_cbar(
        self, start_index: int, end_index: int, high_price: float, low_price: float
    ):
        new_cbar = {
            "index": self.df_cbar.height,
            "start_index": start_index,
            "end_index": end_index,
            "high_price": high_price,
            "low_price": low_price,
            "fractal_type": FractalType.NONE.value,
        }
        self.df_cbar = self.df_cbar.vstack(
            pl.DataFrame([new_cbar], schema=self.df_cbar.schema)
        )

    def _detect_fractal(self):
        # 分形识别
        # region 0. 算法说明
        # 0. 分形：分形由3根相邻K线组成，有顶分形（中间高两边低）和底分形两种（中间低两边高），需要在包含合并处理过的k线中进行
        # 1. 分形定义：每个分形即视为一个分形，
        # 2. 分形判别方法：接收到sbar后，对前一个bar进行顶底分形的判定
        # endregion

        # 分形判断
        # 取最近的3条bar，判断是否为分形
        last_bar_list = self.get_last_cbar(3)
        if last_bar_list is None or len(last_bar_list) != 3:  # k线数量不够，不符合分形判断条数要求
            return

        left_bar, middle_bar, right_bar = last_bar_list

        fractal_type = Fractal.is_fractal(left_bar, middle_bar, right_bar)

        if fractal_type != FractalType.NONE:  # 是分形
            # 是分形，更新分形标识，更新cbar_df数据源
            self.df_cbar = self.df_cbar.with_columns(
                [
                    pl.when(pl.col("index") == middle_bar.index)
                    .then(pl.lit(fractal_type.value))
                    .otherwise(pl.col("fractal_type"))
                    .alias("fractal_type"),
                ]
            )
            # logger.debug("找到分形",{"df_cbar_height":self.df_cbar.height})

    def get_last_cbar(self, count:int = None) -> List[CBar] | CBar | None:
        if count is None:
            count = 1
        df = self.df_cbar.tail(count)
        if df.is_empty():
            return None
        if count == 1:
            return CBar(**df.row(0, named=True))
        return  [CBar(**row) for row in df.rows(named=True)]

    def get_cbar_list(self, start_index:int = None, end_index:int = None)-> List[CBar] | None:
        if start_index is None:
            start_index = 0
        if end_index is None:
            end_index = self.df_cbar.height - 1
        df = self.df_cbar.slice(start_index, end_index - start_index + 1)
        if df.is_empty():
            return None
        return  [CBar(**row) for row in df.rows(named=True)]

    def get_fractal(self, index: int = None) -> Fractal | None:
        if index is None:
            # 取最新的分形
            index = self.df_cbar.filter(pl.col("fractal_type") != FractalType.NONE).tail(1).select(pl.col("index")).item()

        start_index = index - 1
        end_index = index + 1
        rows = self.df_cbar.slice(start_index, end_index - start_index + 1).rows(
            named=True
        )
        if len(rows) != 3:
            return None
        fractal = Fractal(
            left=CBar(**rows[0]), middle=CBar(**rows[1]), right=CBar(**rows[2])
        )
        return fractal if fractal.fractal_type() != FractalType.NONE else None

    def prev_fractal(self, index: int) -> Fractal | None:
        prev_fractal_index = self.df_cbar.filter(
            (pl.col("index") < index) & (pl.col("fractal_type") != FractalType.NONE)).tail(1).select(pl.col("index")).item()
        return self.get_fractal(prev_fractal_index)

    def next_fractal(self, index: int) -> Fractal | None:
        prev_fractal_index = self.df_cbar.filter(
            (pl.col("index") > index) & (pl.col("fractal_type") != FractalType.NONE)).head(1).select(pl.col("index")).item()
        return self.get_fractal(prev_fractal_index)
