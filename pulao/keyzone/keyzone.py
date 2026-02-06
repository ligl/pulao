from __future__ import annotations
from dataclasses import dataclass

from pulao.constant import KeyZoneOrientation, KeyZoneOrigin, Timeframe
from datetime import datetime as Datetime

@dataclass
class KeyZone:
    """
    关键位置，即我感兴趣的潜在交易位置，
    KeyZone 本体结构，不包含任何派生属性（如 accept/reject/category 等）。
    表达 KeyZone 的客观几何结构 + 来源 + 基础强度。
    """

    # 唯一 ID
    id: int = None

    # 来源类型（来源不会变）
    origin_type: KeyZoneOrigin = None
    # 所属周期 5m 15m 1h 1d
    timeframe: Timeframe = None
    # 几何结构类型：'horizontal' / 'trendline' / 'channel'
    orientation: KeyZoneOrientation = None
    # 时间跨度
    sbar_start_id: int = None
    sbar_end_id: int = None
    # 空间边界（无论水平/斜线/通道，都有）
    upper: float = None
    lower: float = None
    # 若 orientation == 'channel' 使用
    trendline_slope: float = None # 若 orientation == 'trendline' 使用
    trendline_intercept: float = None
    channel_line_slope: float = None
    channel_line_intercept: float = None

    # 基础强度（依据来源、影线结构、离开力度等）
    # base_strength: float = 0.0

    # 行为基础数据（统计类，不涉及行为分类）
    touch_count: int = 0
    last_touch_id: int = None

    # 构成 KeyZone 的关键 index（分形点、突破点等）
    # key_indices: List[int] = field(default_factory=list)

    created_at: Datetime = None  # 创建时间

    def __post_init__(self):
        if isinstance(self.origin_type, str):
            self.origin_type = KeyZoneOrigin(self.origin_type)
        if isinstance(self.timeframe, str):
            self.timeframe = Timeframe(self.timeframe)
        if isinstance(self.orientation, int):
            self.orientation = KeyZoneOrientation(self.orientation)

    def is_horizontal(self) -> bool:
        return self.orientation == KeyZoneOrientation.HORIZONTAL

    def is_trendline(self) -> bool:
        return self.orientation == KeyZoneOrientation.TRENDLINE

    def is_channel(self) -> bool:
        return self.orientation == KeyZoneOrientation.CHANNEL

    def contains_price(self, price: float) -> bool:
        """判断某价格是否落在 zone 内"""
        return self.lower <= price <= self.upper

    def overlap(self, other: KeyZone) -> bool:
        raise NotImplementedError

    def merge(self, other: KeyZone, only_overlap:bool=True) -> KeyZone | None:
        """
        合并KeyZone区间
        :param other:
        :param only_overlap: 是否只合并有重叠部分的KeyZone
        :return:
        """
        raise NotImplementedError
