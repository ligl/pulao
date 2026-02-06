from typing import Callable, Any

from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import BarGenerator
from vnpy_ctastrategy import CtaTemplate

from pulao.bar import SBar
from pulao.constant import Timeframe
from pulao.object import BaseDecorator
from pulao.mtc import MultiTimeframeContext

@BaseDecorator()
class PulaoStrategy(CtaTemplate):
    author = "Pulao"

    def __init__(
        self,
        cta_engine: Any,
        strategy_name: str,
        vt_symbol: str,
        setting: dict,
    ):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.mtc = MultiTimeframeContext(symbol=self.vt_symbol)
        self.bg_entry = None
        self.bg_trade = None
        self.bg_trend = None
        self.bg_tick = None

    def on_init(self) -> None:
        # 聚合K线，生成高周期K线
        self.bg_tick = BarGenerator(on_bar=self.on_bar)
        self.bg_trend = BarGenerator(
            on_bar=Callable,
            window=1,
            on_window_bar=self.on_trend_bar,
            interval=Interval.HOUR,
        )
        self.bg_trade = BarGenerator(
            on_bar=Callable,
            window=15,
            on_window_bar=self.on_trade_bar,
            interval=Interval.MINUTE,
        )  # 15分钟合成
        self.bg_entry = BarGenerator(
            on_bar=Callable,
            window=5,
            on_window_bar=self.on_entry_bar,
            interval=Interval.MINUTE,
        )  # 5分钟合成

        self.mtc.register(Timeframe.M5)
        self.mtc.register(Timeframe.M15)
        self.mtc.register(Timeframe.H1)

        # # 订阅行情
        # self.cta_engine.subscribe(self.vt_symbol, self.cta_engine.main_engine.get_default_gateway_name())
        print(f"策略 - {self.__class__.__name__} - on_init")

    def on_start(self) -> None:
        print(f"策略 - {self.__class__.__name__} - on_start")

    def on_stop(self) -> None:
        print(f"策略 - {self.__class__.__name__} - on_stop")

    def on_tick(self, tick: TickData) -> None:
        # print(f"策略 - {self.__class__.__name__} - on_tick")
        # print(f"{tick}")
        self.bg_tick.update_tick(tick)

    def on_bar(self, bar: BarData):
        """K线到来时的核心处理流程"""
        # print(f"策略 - {self.__class__.__name__} - on_bar: {bar}")
        self.bg_entry.update_bar(bar)
        self.bg_trade.update_bar(bar)
        self.bg_trend.update_bar(bar)

    def on_trend_bar(self, bar: BarData):
        # 高周期趋势判断
        print(f"策略 - {self.__class__.__name__} - on_trend_bar: {bar}")
        self.mtc.append(Timeframe(bar.interval.value),self.parse_sbar(bar))

    def on_trade_bar(self, bar: BarData):
        # 中周期信号判断
        print(f"策略 - {self.__class__.__name__} - on_trade_bar: {bar}")
        self.mtc.append(Timeframe(bar.interval.value),self.parse_sbar(bar))

    def on_entry_bar(self, bar: BarData):
        print(f"策略 - {self.__class__.__name__} - on_entry_bar: {bar}")
        # 低周期入场决策
        self.mtc.append(Timeframe(bar.interval.value),self.parse_sbar(bar))

    def execute_decision(self, decision: str, price: float):
        if decision == "open_long":
            if self.pos <= 0:
                self.buy(price, 1)  # 开多
        elif decision == "close_long":
            if self.pos > 0:
                self.sell(price, 1)  # 平多
        elif decision == "open_short":
            if self.pos >= 0:
                self.short(price, 1)  # 开空
        elif decision == "close_short":
            if self.pos < 0:
                self.cover(price, 1)  # 平空
        elif decision == "wait":
            pass  # 等待

    def parse_sbar(self, bar: BarData):
        return SBar(
            symbol=bar.symbol,
            exchange=bar.exchange.value,
            timeframe=Timeframe(bar.interval.value),
            datetime=bar.datetime,
            turnover=bar.turnover,
            open_price=bar.open_price,
            close_price=bar.close_price,
            high_price=bar.high_price,
            low_price=bar.low_price,
            volume=bar.volume,
            open_interest=bar.open_interest,
        )
