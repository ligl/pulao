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
    PARQUET_PATH:str = "../dataset/{symbol}/{filename}.parquet"


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

class Direction(BaseEnum):
    """
    波段/趋势方向，也包含逻辑方向，只要是跟走势相差的方向属性都可以用
    """
    UP = 1
    DOWN = -1
    RANGE = 2
    NONE = 0
    @property
    def opposite(self):
        if self == Direction.UP:
            return Direction.DOWN
        elif self == Direction.DOWN:
            return Direction.UP
        else:
            return Direction.NONE

class DecisionAction(BaseEnum):
    """
    决策行为：多开(buy)、多平(sell)、空开(short)、空平(cover)、等待(wait)
    """
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    COVER = "cover"
    WAIT = "wait"


class KeyZoneOrigin(BaseEnum):
    """
    KeyZone 产生的结构来源
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

class KeyZoneOrientation(BaseEnum):
    HORIZONTAL = 1
    TRENDLINE = 2
    CHANNEL = 3

class EventType(BaseEnum):
    """
    Event事件类型
    """
    SBAR_CREATED = "bar.created"
    CBAR_CHANGED = "cbar.changed"
    SWING_CHANGED = "swing.changed"
    TREND_CHANGED = "trend.changed"
    MTC_NEW_BAR = "mtc.new_bar"
    TIMEFRAME_END = "mtc.timeframe.end"

class Timeframe(BaseEnum):
    """
    时间周期
    """
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    D1 = "1d"
