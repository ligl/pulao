from typing import Optional, Any

from pulao.bar.sbar import SBar
from pulao.bar.sbar_manager import SBarManager
from pulao.bar.cbar_manager import CBarManager
from pulao.constant import Timeframe, EventType
from pulao.events import Observable
from pulao.swing.swing_manager import SwingManager
from pulao.trend.trend_manager import TrendManager


class MultiTimeframeContext(Observable):
    """
    多周期上下文管理器
    统一提供多周期数据访问、缓存、事件触发、历史窗口同步的基础设施。
    """

    def __init__(self):
        super().__init__()
        self.data_manager:dict[Timeframe,dict[str,Observable]] = dict()
        self.last_sbar:Optional[SBar] = None

    def register(self, timeframe:Timeframe, sbar_manager:SBarManager, cbar_manager:CBarManager, swing_manager:SwingManager, trend_manager:TrendManager):
        self.data_manager[timeframe] = dict(sbar_manager=sbar_manager,
                                            cbar_manager=cbar_manager,
                                            swing_manager=swing_manager,
                                            trend_manager=trend_manager)
        sbar_manager.subscribe(self._on_sbar) # 一个new bar过来之后，这些事件会顺序执行，mtc经过处理后，在最后一个事件_on_trend发布出去
        cbar_manager.subscribe(self._on_cbar)
        swing_manager.subscribe(self._on_swing)
        trend_manager.subscribe(self._on_trend)

    def unregister(self, timeframe:Timeframe):
        del self.data_manager[timeframe]

    # ------- 事件接口（从各周期结构流接收新事件） -------
    def _on_sbar(self, timeframe:Timeframe, event: EventType, payload: Any):
        self.last_sbar = payload.get("sbar", None)

    def _on_cbar(self,timeframe:Timeframe, event: EventType, payload: Any):
        pass

    def _on_swing(self, timeframe:Timeframe, event: EventType,  payload: Any):
        pass

    def _on_trend(self, timeframe:Timeframe, event: EventType,  payload: Any):
        # 经过mtc处理之后，发出new bar到来的事件通知
        self.notify( timeframe, EventType.MTC_NEW_BAR, sbar=self.last_sbar)

    # ------- 内部方法 -------


    # ------- 最高层接口（给 keyzone / supply / decision） -------
    def get_context(self):
        """
        返回：一个“快照”结构，包含交易周期及其上下级周期的结构信息
        """
        raise NotImplementedError()
    
