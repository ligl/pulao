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
