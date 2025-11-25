# -*- coding: utf-8 -*-
import polars as pl
from enum import Enum
from typing import Optional, List, Any
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class FractalType(Enum):
    NONE = 0
    TOP = 1      # 顶分形
    BOTTOM = 2   # 底分形

class SwingDirection(Enum):
    UP = 1
    DOWN = -1

    @property
    def opposite(self):
        return SwingDirection.DOWN if self == SwingDirection.UP else SwingDirection.UP

# 简易K线对象（实盘中替换为你自己的Bar类）
class Bar:
    def __init__(self, index: int, open: float, high: float, low: float, close: float):
        self.index = index
        self.open = open
        self.high = high
        self.low = low
        self.close = close

class Fractal:
    def __init__(self, left: Bar, middle: Bar, right: Bar):
        self.left = left
        self.middle = middle
        self.right = right
        self.index = middle.index
        self.high_price = middle.high
        self.low_price = middle.low

    def valid(self) -> FractalType:
        if (self.middle.high >= self.left.high and
            self.middle.high >= self.right.high and
            self.middle.low >= self.left.low and
            self.middle.low >= self.right.low):
            return FractalType.TOP
        if (self.middle.high <= self.left.high and
            self.middle.high <= self.right.high and
            self.middle.low <= self.left.low and
            self.middle.low <= self.right.low):
            return FractalType.BOTTOM
        return FractalType.NONE

    def __repr__(self):
        t = self.valid()
        return f"Fractal({t.name if t != FractalType.NONE else 'NONE'}, idx={self.index}, H={self.high_price}, L={self.low_price})"


class Swing:
    def __init__(self, **kwargs):
        self.index: int = kwargs["index"]
        self.direction: SwingDirection = kwargs["direction"]
        self.start_index: int = kwargs["start_index"]
        self.end_index: int = kwargs["end_index"]
        self.high_price: float = kwargs["high_price"]
        self.low_price: float = kwargs["low_price"]
        self.is_completed: bool = kwargs.get("is_completed", False)

    @property
    def distance(self) -> float:
        return abs(self.high_price - self.low_price)

    def __repr__(self):
        return f"Swing({self.direction.name}, {self.start_index}→{self.end_index}, " \
               f"H{self.high_price:.2f}-L{self.low_price:.2f}, 完成={self.is_completed})"


class ChanBiDetector:
    """
    实盘级缠论笔实时划分器
    strict_mode=False  → 人眼模式（推荐实盘）
    strict_mode=True   → 严格模式（仅用于教学）
    """
    def __init__(self, strict_mode: bool = False,
                 min_swing_bars: int = 6,       # 一笔最少包含K线数（合并后）
                 min_swing_points: float = 0):  # 一笔最小点数（根据品种设置，如沪深300设50，BTC设300）
        self.strict_mode = strict_mode
        self.min_swing_bars = min_swing_bars
        self.min_swing_points = min_swing_points

        # 合并后的K线序列（包含处理后）
        self.cbars: List[Bar] = []

        # 存储所有笔（DataFrame方便操作）
        self.df_swing = pl.DataFrame(schema={
            "index": pl.Int64,
            "direction": pl.Int8,
            "start_index": pl.Int64,
            "end_index": pl.Int64,
            "high_price": pl.Float64,
            "low_price": pl.Float64,
            "is_completed": pl.Boolean,
        })

    def feed(self, bar: Any):
        """主入口：每来一根新K线调用一次"""
        if not hasattr(bar, 'index') or not all(hasattr(bar, x) for x in ['open','high','low','close']):
            raise ValueError("bar 必须包含 index, open, high, low, close")

        new_bar = Bar(
            index=bar.index,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close
        )

        # 1. 包含处理
        merged_bar = self._include_process(new_bar)
        self.cbars.append(merged_bar)

        # 2. 构建笔
        self._build_swing()

    def _include_process(self, new_bar: Bar) -> Bar:
        """经典包含处理（同向处理 + 被包含处理）"""
        if not self.cbars:
            return new_bar

        last = self.cbars[-1]

        # 新K被旧K完全包含 → 丢弃
        if last.high >= new_bar.high and last.low <= new_bar.low:
            return last

        # 旧K被新K完全包含 → 替换
        if last.high <= new_bar.high and last.low >= new_bar.low:
            self.cbars[-1] = new_bar
            return new_bar

        # 同向包含处理（核心！）
        if (last.close > last.open) == (new_bar.close > new_bar.open):  # 同向
            if last.close > last.open:  # 都是阳线
                if new_bar.low <= last.low:
                    new_bar.high = max(last.high, new_bar.high)
                    new_bar.low = min(last.low, new_bar.low)
                    self.cbars[-1] = new_bar
                    return new_bar
            else:  # 都是阴线
                if new_bar.high >= last.high:
                    new_bar.high = max(last.high, new_bar.high)
                    new_bar.low = min(last.low, new_bar.low)
                    self.cbars[-1] = new_bar
                    return new_bar

        return new_bar

    def _build_swing(self):
        if len(self.cbars) < 5:
            return

        left, mid, right = self.cbars[-3], self.cbars[-2], self.cbars[-1]
        curr_fractal = Fractal(left, mid, right)
        ftype = curr_fractal.valid()

        # 每根K都更新未完成笔的高低点
        self._update_active_high_low(right)

        if ftype == FractalType.NONE:
            return

        # 1. 首次建立笔
        if self.df_swing.is_empty():
            direction = SwingDirection.DOWN if ftype == FractalType.TOP else SwingDirection.UP
            self._append_swing(direction, curr_fractal, completed=False)
            logger.info(f"首次建立笔: {direction.name}")
            return

        active = self.get_active_swing()
        if not active:
            return

        start_fractal = self._get_fractal_at(active.start_index)
        if not start_fractal:
            return

        # 2. 关键：起点分型是否失效（被吃掉）
        if self._is_start_fractal_invalidated(active, start_fractal, curr_fractal, right):
            logger.info(f"分型失效！回滚并重新建立笔")
            self.df_swing = self.df_swing.slice(0, self.df_swing.height - 1)
            direction = SwingDirection.DOWN if ftype == FractalType.TOP else SwingDirection.UP
            self._append_swing(direction, curr_fractal, completed=False)
            return

        # 3. 尝试成笔
        prev_swing = self._get_last_completed_swing()
        if self._can_complete_swing(active, start_fractal, curr_fractal, prev_swing):
            active.end_index = curr_fractal.index
            active.high_price = max(active.high_price, curr_fractal.high_price)
            active.low_price = min(active.low_price, curr_fractal.low_price)
            active.is_completed = True
            self._replace_active_swing(active)

            # 立即开启新笔
            new_dir = SwingDirection.DOWN if ftype == FractalType.TOP else SwingDirection.UP
            self._append_swing(new_dir, curr_fractal, completed=False)
            logger.info(f"成笔成功 → {active.direction.name}笔完成，新{new_dir.name}笔启动")
        else:
            # 延续当前笔
            active.end_index = right.index
            self._replace_active_swing(active)

    def _can_complete_swing(self, active: Swing, start_fractal: Fractal,
                            end_fractal: Fractal, prev_swing: Optional[Swing]) -> bool:
        # 严格模式优先
        if self.strict_mode:
            return self._can_complete_strict(active, start_fractal, end_fractal)

        # 人眼模式：先严格，再弱化
        if self._can_complete_strict(active, start_fractal, end_fractal):
            return True

        return self._can_complete_weak(start_fractal, end_fractal, prev_swing)

    def _can_complete_strict(self, active: Swing, start_fractal: Fractal, end_fractal: Fractal) -> bool:
        if active.direction == SwingDirection.UP:
            return end_fractal.valid() == FractalType.TOP and end_fractal.low_price > start_fractal.high_price
        else:
            return end_fractal.valid() == FractalType.BOTTOM and end_fractal.high_price < start_fractal.low_price

    def _can_complete_weak(self, start_fractal: Fractal, end_fractal: Fractal,
                           prev_swing: Optional[Swing]) -> bool:
        """实盘最强人眼成笔规则"""
        if not prev_swing or prev_swing.distance <= 0:
            return False

        if start_fractal.valid() == end_fractal.valid():
            return False

        # 计算两个分形之间的真实波动幅度
        amp_high = max(start_fractal.high_price, end_fractal.high_price)
        amp_low = min(start_fractal.low_price, end_fractal.low_price)
        curr_amplitude = amp_high - amp_low

        # 规则1：完全不重叠 → 直接成笔
        if start_fractal.high_price < end_fractal.low_price or start_fractal.low_price > end_fractal.high_price:
            return True

        # 规则2：有重叠，但时间 + 力度足够
        bars_between = abs(end_fractal.index - start_fractal.index) - 1
        if bars_between < self.min_swing_bars:
            return False

        if curr_amplitude < prev_swing.distance * 0.54:  # 54% 是实盘最优值
            return False

        if self.min_swing_points and curr_amplitude < self.min_swing_points:
            return False

        return True

    def _is_start_fractal_invalidated(self, active: Swing, start_fractal: Fractal,
                                      curr_fractal: Fractal, latest_bar: Bar) -> bool:
        if active.direction == SwingDirection.UP:   # 起点是底
            return (curr_fractal.valid() == FractalType.BOTTOM and curr_fractal.low_price < start_fractal.low_price) or \
                   latest_bar.low < start_fractal.low_price
        else:
            return (curr_fractal.valid() == FractalType.TOP and curr_fractal.high_price > start_fractal.high_price) or \
                   latest_bar.high > start_fractal.high_price

    def _update_active_high_low(self, bar: Bar):
        active = self.get_active_swing()
        if active and not active.is_completed:
            updated = False
            if bar.high > active.high_price:
                active.high_price = bar.high
                updated = True
            if bar.low < active.low_price:
                active.low_price = bar.low
                updated = True
            if updated:
                self._replace_active_swing(active)

    def _append_swing(self, direction: SwingDirection, fractal: Fractal, completed: bool):
        new_row = {
            "index": self.df_swing.height,
            "direction": direction.value,
            "start_index": fractal.index,
            "end_index": fractal.index,
            "high_price": fractal.high_price,
            "low_price": fractal.low_price,
            "is_completed": completed,
        }
        self.df_swing = self.df_swing.vstack(pl.DataFrame([new_row], schema=self.df_swing.schema))

    def _replace_active_swing(self, swing: Swing):
        if self.df_swing.is_empty():
            return
        self.df_swing = self.df_swing.slice(0, self.df_swing.height - 1)
        self._append_swing(swing.direction,
                           type('F', (), {'index': swing.start_index, 'high_price': swing.high_price, 'low_price': swing.low_price})(),
                           swing.is_completed)
        # 更新字段
        last_idx = self.df_swing.height - 1
        self.df_swing = self.df_swing.with_columns([
            pl.when(pl.col("index") == last_idx).then(swing.end_index).otherwise(pl.col("end_index")).alias("end_index"),
            pl.when(pl.col("index") == last_idx).then(swing.high_price).otherwise(pl.col("high_price")).alias("high_price"),
            pl.when(pl.col("index") == last_idx).then(swing.low_price).otherwise(pl.col("low_price")).alias("low_price"),
            pl.when(pl.col("index") == last_idx).then(swing.is_completed).otherwise(pl.col("is_completed")).alias("is_completed"),
        ])

    def get_active_swing(self) -> Optional[Swing]:
        if self.df_swing.is_empty():
            return None
        row = self.df_swing.row(-1, named=True)
        if row["is_completed"]:
            return None
        return Swing(**row)

    def _get_last_completed_swing(self) -> Optional[Swing]:
        if self.df_swing.height < 2:
            return None
        for i in range(self.df_swing.height - 2, -1, -1):
            row = self.df_swing.row(i, named=True)
            if row["is_completed"]:
                return Swing(**row)
        return None

    def _get_fractal_at(self, index: int) -> Optional[Fractal]:
        for i in range(len(self.cbars) - 2):
            f = Fractal(self.cbars[i], self.cbars[i+1], self.cbars[i+2])
            if f.valid() != FractalType.NONE and f.index == index:
                return f
        return None

    def get_all_swings(self) -> List[Swing]:
        return [Swing(**row) for row in self.df_swing.iter_rows(named=True)]

    def get_completed_swings(self) -> List[Swing]:
        return [s for s in self.get_all_swings() if s.is_completed]
