from abc import ABC, abstractmethod
from typing import Any, List

from pulao.constant import Timeframe, KeyZoneOrigin, Direction
from pulao.keyzone import KeyZone
from pulao.mtc import MultiTimeframeContext
import polars as pl

from pulao.symbol.registry import SymbolRegistry


def compute_multi_touch(df_sub: pl.DataFrame, tick_size: float, direction: Direction) -> (float, float, list):
    """
    多触碰 KeyZone
    direction = "low"
    lower = 最小值
    upper = 实体底部与下影线之间触碰最多的值
    direction = "high"
    lower = 实体顶部与上影线之间触碰最多的值
    upper = 最大值
    参数：
        df_sub: polars.DataFrame 或任何有 open_price, close_price, high_price, low_price 的 DataFrame
        tick: 价格最小刻度
        direction: "high" = 高点 KeyZone，"low" = 低点 KeyZone

    返回：
        lower, upper, freq_list [(price, count), ...]
    """

    # 转 Python 列表
    lower = upper = None
    lows = df_sub["low_price"].to_list()
    highs = df_sub["high_price"].to_list()
    opens = df_sub["open_price"].to_list()
    closes = df_sub["close_price"].to_list()

    if direction == Direction.UP:
        # -------- 高点 KeyZone --------
        upper = max(highs)
        min_body_top = min(max(o, c) for o, c in zip(opens, closes))
        start_price = min_body_top
        end_price = upper
    elif direction == Direction.DOWN:
        # -------- 低点 KeyZone --------
        lower = min(lows)
        max_body_bottom = max(min(o, c) for o, c in zip(opens, closes))
        start_price = lower
        end_price = max_body_bottom
    else:
        raise ValueError("direction must be 'high' or 'low'")

    # -------- 构建价格网格 --------
    price_grid = []
    p = start_price
    while p <= end_price:
        price_grid.append(p)
        p += tick_size

    # -------- 统计触碰次数 --------
    counts = []
    for price in price_grid:
        cnt = 0
        for lo, hi in zip(lows, highs):
            if lo <= price <= hi:
                cnt += 1
        counts.append(cnt)

    # -------- 确定 lower/upper --------
    max_idx = counts.index(max(counts))
    best_price = price_grid[max_idx]

    if direction == Direction.UP:
        lower = best_price
        # upper 已经是最高影线
    else:  # low
        upper = best_price
        # lower 已经是最低影线

    freq_list = list(zip(price_grid, counts))

    return lower, upper, freq_list


class KeyZoneBuilder(ABC):
    """
    所有 KeyZone builder 类型的抽象基类
    """

    origin_type: KeyZoneOrigin

    def __init__(self, mtc: MultiTimeframeContext, timeframe: Timeframe):
        self.mtc = mtc
        self.timeframe = timeframe
        self.symbol = mtc.symbol

    @abstractmethod
    def build(self) -> List[KeyZone] | None:
        """
        不同 origin_type 必须实现自己的构建方法
        """
        pass

    def get_upper_lower(
        self, pivot_id: int, length: int, direction: Direction
    ) -> (float, float, list):
        sbar_df = self.mtc.get_around_sbar(pivot_id, length, self.timeframe, ret_df=True)
        symbol = SymbolRegistry.get(self.symbol)
        return compute_multi_touch(sbar_df, symbol.tick_size, direction)
