from abc import ABC, abstractmethod
from typing import List, Optional, Dict

from pulao.constant import KeyZoneOrigin, KeyZoneType
from pulao.object import Base
from pulao.swing import Swing
from pulao.trend import Trend

class KeyZone(Base,ABC):
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
