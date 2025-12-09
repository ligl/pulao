from dataclasses import dataclass, field
from typing import List, Optional, Dict

from .base import Symbol, TradingSession


@dataclass(frozen=True)
class FeeModel:
    """
    手续费模型：
    1. per_lot：按手收取
    2. by_value：按成交金额收取（比例）
    3. maker/taker：支持数字货币期货扩展
    """
    per_lot: float = 0.0
    by_value_rate: float = 0.0
    maker_rate: float = 0.0
    taker_rate: float = 0.0


@dataclass(frozen=True)
class FutureSymbol(Symbol):
    """
    完整的期货合约属性定义
    """
    # 基础属性（必备）
    product: str                   # 品种代码，如 "rb"
    multiplier: float              # 合约乘数，如 10
    margin_rate: float             # 保证金比率（占用）
    delivery_month: Optional[str]  # 交割月份 "2505"；主连则为 None
    trading_unit: str              # 手数，如 "10吨/手"

    asset_type: str = field(default="future", init=False)
    # 涨跌停属性（可选）
    limit_rate: float = 0.06       # 涨跌停板幅度比率

    # 手续费模型
    fee: FeeModel = field(default_factory=FeeModel)

    # 扩展属性
    is_main_contract: bool = False
    is_continuous: bool = False      # 是否是连续合约，如 rb888

    # CTP 通用属性
    volume_multiple: int = 1         # 成交量倍数（一般为1）
    max_limit: Optional[float] = None
    min_limit: Optional[float] = None

    # 品种级额外属性
    category: Optional[str] = None   # “黑色系/有色/农产品/化工...”

    def calc_fee(self, price: float, volume: float) -> float:
        """
        自动选择手续费模型计算方式
        """
        by_lot = self.fee.per_lot * volume
        by_value = price * volume * self.multiplier * self.fee.by_value_rate
        return by_lot + by_value

    def tick_value(self) -> float:
        return self.tick_size * self.multiplier

    def limit_up(self, last_price: float) -> float:
        return last_price * (1 + self.limit_rate)

    def limit_down(self, last_price: float) -> float:
        return last_price * (1 - self.limit_rate)

    def margin(self, price: float) -> float:
        return price * self.multiplier * self.margin_rate
