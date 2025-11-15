from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Dict
from pulao.events import Observable
from pulao.swing import Swing
from pulao.tend import Trend


class KeyZoneType(Enum):
    """
    KeyZone 的类别或功能类型，比如 support、resistance、oscillation 等，决定 KeyZone 在分析和逻辑上的行为
    """

    SUPPORT = "support"  # 支撑
    RESISTANCE = "resistance"  # 阻力
    COUNTER_PRESSURE = "counter_pressure"  # 反压位
    PULLBACK = "pullback"  # 回踩位
    BREAKOUT = "breakout"  # 突破位
    REVERSAL = "reversal"  # 反转点
    SUPPLY_DEMAND = "supply_demand"  # 供需区
    RANGE_BORDER = "range_border"  # 区间边界
    UNKNOWN = "unknown"  # 未知


class KeyZoneOrigin(Enum):
    """
    KeyZone 的来源或生成逻辑，比如 from_trend、from_swing、from_manual，帮助追踪这个区域是通过哪种方式产生的（便于管理、更新或过滤）

    """

    MAJOR_SWING = "major_swing"  # 主波段
    SECONDARY_SWING = "secondary_swing"  # 次级波段
    TREND_HH = "trend_hh"  # 结构高点 HH
    TREND_HL = "trend_hl"  # 结构低点 HL
    TREND_LL = "trend_ll"  # 结构低点 LL
    TREND_LH = "trend_lh"  # 结构高点 LH
    EMA = "ema"  # EMA 动态关键位
    ATR = "atr"  # 波动率（ATR 区）
    VOLUME_NODE = "volume_node"  # 成交量节点（POC / value area）
    CANDLE_ACTION = "candle_action"  # K线行为区（长影线、多次测试）
    UNKNOWN = "unknown"  # 未知


class KeyZone(ABC):
    start_index: int
    end_index: int
    price_low: float
    price_high: float
    trend_ref: Optional["Trend"]
    swings_ref: Optional[List["Swing"]]
    origin_type: KeyZoneOrigin  # 来源类型，如 'from_swing', 'from_trend', 'from_manual'
    meta: Dict

    def __init__(
        self,
        start_index: int,
        end_index: int,
        price_low: float,
        price_high: float,
        trend_ref: Optional["Trend"] = None,
        swings_ref: Optional[List["Swing"]] = None,
        origin_type: KeyZoneOrigin = KeyZoneOrigin.UNKNOWN,
    ):
        self.start_index = start_index
        self.end_index = end_index
        self.price_low = price_low
        self.price_high = price_high
        self.trend_ref = trend_ref
        self.swings_ref = swings_ref or []
        self.origin_type = origin_type
        self.meta = {}

    @property
    @abstractmethod
    def zone_type(self) -> KeyZoneType:
        """KeyZone类别，由子类实现，如 support / resistance / oscillation"""

    @abstractmethod
    def update_zone(self, new_trend: "Trend"):
        """更新KeyZone逻辑，子类实现"""

    def contains(self, price: float) -> bool:
        """判断价格是否在KeyZone区域内"""
        return self.price_low <= price <= self.price_high

    def merge_with(self, other: "KeyZone"):
        """与同类型KeyZone合并"""
        self.price_low = min(self.price_low, other.price_low)
        self.price_high = max(self.price_high, other.price_high)

    def to_dict(self) -> Dict:
        return {
            "start_index": self.start_index,
            "end_index": self.end_index,
            "price_low": self.price_low,
            "price_high": self.price_high,
            "origin_type": self.origin_type,
            "meta": self.meta,
        }


class KeyZoneManager(Observable):
    key_zones: List[KeyZone]

    def __init__(self):
        super().__init__()
        self.key_zones = []

    def add(self, key_zone: KeyZone):
        self.key_zones.append(key_zone)
        self.notify("key_zone.created", key_zone)
