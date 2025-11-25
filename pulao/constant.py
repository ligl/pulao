from enum import Enum

class ReadOnlyMeta(type):
    def __call__(cls, *args, **kwargs):
        raise TypeError(f"Class: {cls.__name__} cannot be instantiated")

    def __setattr__(cls, name, value):
        raise AttributeError(f"Class attribute: '{name}' is read-only")

class Const(metaclass=ReadOnlyMeta):
    DEBUG: bool = True  # 系统模式
    LOOKBACK_LIMIT: int = (
        300  # 检查前一个波段/趋势时，向前回溯的K线数量，越过这个数量就不再关注
    )


class BaseEnum(Enum):
    def __repr__(self):
        return str(self.value)

    def __str__(self):
        return self.value

    def __eq__(self, other):
        # 如果 other 的类型与 value 相同，则按 value 比较
        if isinstance(other, Enum):
            return self.value == other.value
        return self.value == other

    def __ne__(self, other):
        # 明确返回不等于 __eq__ 的否定
        return not self.__eq__(other)

    def __hash__(self):
        return super().__hash__()

    @classmethod
    def parse(cls, value: str):
        for member in cls:
            if str(member.value).lower() == value.lower():
                return member

        raise ValueError(f"{value!r} is not a valid {cls.__name__}")

class FractalType(BaseEnum):
    """
    分形类型
    """
    TOP = 1
    BOTTOM = -1
    NONE = 0

class SwingDirection(BaseEnum):
    """
    波段方向
    """
    UP = 1
    DOWN = -1

    @property
    def opposite(self):
        return SwingDirection.DOWN if self == SwingDirection.UP else SwingDirection.UP

class TrendDirection(BaseEnum):
    """
    趋势方向
    """
    UP = 1
    DOWN = -1
    RANGE = 2


class DecisionAction(BaseEnum):
    """
    决策行为：多开(buy)、多平(sell)、空开(short)、空平(cover)、等待(wait)
    """
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    COVER = "cover"
    WAIT = "wait"


class KeyZoneType(BaseEnum):
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


class KeyZoneOrigin(BaseEnum):
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


class EventType(BaseEnum):
    """
    Event事件类型
    """
    SBAR_CREATED = "bar.created"
    CBAR_CREATED = "cbar.created"
    SWING_CHANGED = "swing.changed"
    TREND_CHANGED = "trend.changed"
