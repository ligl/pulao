from typing import Callable

from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import BarGenerator
from vnpy_ctastrategy import CtaTemplate
from pulao.object import PulaoBar, Structure, KeyZone, Signal, Decision


class PulaoStrategy(CtaTemplate):  # noqa: SPELLING
    author = "Pulao"  # noqa: SPELLING

    def on_init(self) -> None:
        # 初始化蒲牢系统模块
        self.data = None  # 数据处理
        self.structure = Structure()  # 波段结构
        self.keyzone = KeyZone()  # 关键位置 # noqa: SPELLING
        self.signal = Signal()  # 信号分析
        self.decision = Decision()  # 决策模块

        self.bg_tick = BarGenerator(
            on_bar=self.on_bar
        )
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

        # # 订阅行情
        # self.cta_engine.subscribe(self.vt_symbol, self.cta_engine.main_engine.get_default_gateway_name())
        print(f"策略 - {self.__class__.__name__} - on_init")

    def on_start(self) -> None:
        print(f"策略 - {self.__class__.__name__} - on_start")

    def on_stop(self) -> None:
        print(f"策略 - {self.__class__.__name__} - on_stop")

    def on_tick(self, tick: TickData) -> None:
        # print(f"策略 - {self.__class__.__name__} - on_tick")
        #print(f"{tick}")
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
        # self.trend_state = self.structure.update(PulaoBar(bar))

    def on_trade_bar(self, bar: BarData):
        # 中周期信号判断
        print(f"策略 - {self.__class__.__name__} - on_trade_bar: {bar}")
        # if self.trend_state is None:
        #     return
        # self.keyzones = self.keyzone.update(bar, self.trend_state)
        # self.signal_state = self.signal.update(bar, self.keyzones, self.trend_state)

    def on_entry_bar(self, bar: BarData):
        print(f"策略 - {self.__class__.__name__} - on_entry_bar: {bar}")
        # 低周期入场决策
        # if not all([self.trend_state, self.keyzones, self.signal_state]):
        #     return
        # decision_result = self.decision.evaluate(
        #     self.trend_state, self.keyzones, self.signal_state
        # )
        # self.execute_decision(decision_result["decision"], bar.close_price)

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
