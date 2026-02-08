from abc import ABC
from dataclasses import dataclass
from typing import List


@dataclass(slots=True)
class TradingSession:
    """
    交易时间段，例如：
    [
        ("09:00", "10:15"),
        ("10:30", "11:30"),
        ("13:30", "15:00"),
        ("21:00", "23:00")
    ]
    """
    sections: List[tuple]     # (start, end)

@dataclass(slots=True)
class Symbol(ABC):
    asset_type: str # 类型：future / stock / crypto / crypto_contract
    code: str # 该合约/标的的唯一名称（rb2505 / AAPL / BTC）
    name: str # 显示名字
    exchange: str # 交易所
    tick_size: float # 最小报价单位
    price_precision: int # 小数位精度
    currency: str # 计价货币
    sessions: TradingSession

    def round_price(self, price: float) -> float:
        """
        根据 tick_size 与 price_precision 修正价格。
        """
        rounded = round(price / self.tick_size) * self.tick_size
        return round(rounded, self.price_precision)
