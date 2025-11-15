from enum import Enum


class SwingPoint(Enum):
    """
    波段高低点
    """

    HIGH = "high"
    LOW = "low"
    NONE = ""


class SwingDirection(Enum):
    """
    波段方向
    """

    UP = "up"
    DOWN = "down"
    NONE = ""


class TrendDirection(Enum):
    """
    趋势方向
    """

    UP = "up"
    DOWN = "down"
    RANGE = "range"
    NONE = ""


class DecisionAction(Enum):
    """
    决策行为：多开(buy)、多平(sell)、空开(short)、空平(cover)、等待(wait)
    """

    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    COVER = "cover"
    WAIT = "wait"


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
