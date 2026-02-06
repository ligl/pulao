from __future__ import annotations

from collections import defaultdict
from typing import Optional, Any, List

from pulao.bar.cbar import CBar
from pulao.bar import SBar
from pulao.bar.sbar_manager import SBarManager
from pulao.bar.cbar_manager import CBarManager
from pulao.constant import Timeframe, EventType
from pulao.events import Observable
from pulao.swing.swing import Swing
from pulao.swing.swing_manager import SwingManager
from pulao.trend import Trend
from pulao.trend.trend_manager import TrendManager
import polars as pl


class MultiTimeframeContext(Observable):
    """
    多周期上下文管理器
    统一提供多周期数据访问、缓存、事件触发、历史窗口同步的基础设施。
    """

    def __init__(self, symbol: str):
        super().__init__()
        self.symbol = symbol
        self.data_manager: Optional[dict[Timeframe, _TimeframeMgr]] = defaultdict()

    def register(self, timeframe: Timeframe):
        timeframe_mgr = _TimeframeMgr(self, timeframe)
        timeframe_mgr.subscribe(self._on_new_bar, EventType.TIMEFRAME_END)
        self.data_manager[timeframe] = timeframe_mgr
        return self.data_manager[timeframe]

    def unregister(self, timeframe: Timeframe):
        del self.data_manager[timeframe]

    def _on_new_bar(self, timeframe: Timeframe, event: EventType, payload: Any):
        # 每个单周期的数据流执行完毕后调用
        # 经过mtc处理之后，发出new bar到来的事件通知
        self.notify(timeframe, EventType.MTC_NEW_BAR, info="单周期的数据流执行完毕")

    def append(self, timeframe: Timeframe, sbar: SBar):
        self.data_manager[timeframe].sbar_manager.append(sbar)

    def get_manager(self, timeframe: Timeframe):
        return self.data_manager[timeframe]

    def get_sbar_window(
        self, length: int, timeframe: Timeframe
    ) -> List[SBar] | SBar | None:
        return self.data_manager[timeframe].sbar_manager.get_last_sbar(length)

    def get_cbar_window(
        self, length: int, timeframe: Timeframe
    ) -> List[CBar] | None | CBar:
        return self.data_manager[timeframe].cbar_manager.get_last_cbar(length)

    def get_swing_window(
        self, length: int, timeframe: Timeframe
    ) -> List[Swing] | Swing | None:
        return self.data_manager[timeframe].swing_manager.get_last_swing(length)

    def get_trend_window(
        self, length: int, timeframe: Timeframe
    ) -> List[Trend] | Trend | None:
        return self.data_manager[timeframe].trend_manager.get_last_trend(length)

    def get_around_sbar(self, pivot_id:int, length: int, timeframe: Timeframe, ret_df:bool)  -> List[SBar] | None| pl.DataFrame:
        return self.data_manager[timeframe].sbar_manager.get_around_sbar(pivot_id, length, ret_df)

class _TimeframeMgr(Observable):
    """
    单周期数据管理
    """

    def __init__(self, mtc: MultiTimeframeContext, timeframe: Timeframe):
        super().__init__()
        sbar_manager = SBarManager(symbol=mtc.symbol, timeframe=timeframe)
        cbar_manager = CBarManager(sbar_manager=sbar_manager)
        swing_manager = SwingManager(cbar_manager=cbar_manager)
        trend_manager = TrendManager(swing_manager=swing_manager)
        # 一个new bar过来之后，这些事件会顺序执行，mtc经过处理后，在最后一个事件_on_trend发布出去
        sbar_manager.subscribe(self._on_new_sbar, EventType.SBAR_CREATED)
        cbar_manager.subscribe(self._on_cbar_changed, EventType.CBAR_CHANGED)
        swing_manager.subscribe(self._on_swing_changed, EventType.SWING_CHANGED)
        trend_manager.subscribe(self._on_trend_changed, EventType.TREND_CHANGED)

        self.sbar_manager = sbar_manager
        self.cbar_manager = cbar_manager
        self.swing_manager = swing_manager
        self.trend_manager = trend_manager

        self.mtc = mtc
        self.timeframe = timeframe
        self.last_sbar: Optional[SBar] = None

    def _on_new_sbar(self, timeframe: Timeframe, event: EventType, payload: Any):
        self.last_sbar = payload.get("sbar", None)

    def _on_cbar_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        pass

    def _on_swing_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        pass

    def _on_trend_changed(self, timeframe: Timeframe, event: EventType, payload: Any):
        # 单周期数据流已经处理结束，向mtc发出new bar到来的事件通知
        self.notify(timeframe, EventType.TIMEFRAME_END, sbar=self.last_sbar)
