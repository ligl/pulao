from dataclasses import dataclass, field
from .base import Symbol


@dataclass(slots=True)
class CryptoContractSymbol(Symbol):
    asset_type: str = field(default="crypto_contract", init=False)
    multiplier: float
    fee_rate: float

    def tick_value(self) -> float:
        return 0.0

    def fee(self, price: float, volume: float) -> float:
        nominal = price * volume * self.multiplier
        return nominal * self.fee_rate
