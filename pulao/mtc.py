from typing import Optional, Any

from pulao.bar import SBarManager, CBarManager
from pulao.constant import Timeframe, EventType
from pulao.events import Observable
from pulao.swing import SwingManager
from pulao.trend import TrendManager


class MultiTimeframeContext(Observable):
    """
    多周期上下文管理器
    用于收集并融合多个周期的结构数据（cbar/swing/trend）
    并对外提供统一的 get_context() 接口
    """

    def __init__(self):
        super().__init__()
        self.tf_mgr:dict[Timeframe,dict[str,Observable]] = dict()

    def register(self, timeframe:Timeframe, sbar_manager:SBarManager, cbar_manager:CBarManager, swing_manager:SwingManager, trend_manager:TrendManager):
        self.tf_mgr[timeframe] = dict(sbar_manager=sbar_manager,
                                      cbar_manager=cbar_manager,
                                      swing_manager=swing_manager,
                                      trend_manager=trend_manager)
        sbar_manager or sbar_manager.subscribe(self._on_sbar)
        cbar_manager or cbar_manager.subscribe(self._on_cbar)
        swing_manager or swing_manager.subscribe(self._on_swing)
        trend_manager or trend_manager.subscribe(self._on_trend)

    def unregister(self, timeframe:Timeframe):
        del self.tf_mgr[timeframe]

    # ------- 事件接口（从各周期结构流接收新事件） -------
    def _on_sbar(self, timeframe:Timeframe, event: EventType, payload: Any):
      self.notify( timeframe, event, **payload)

    def _on_cbar(self,timeframe:Timeframe, event: EventType, payload: Any):
      self.notify( timeframe, event, **payload)

    def _on_swing(self, timeframe:Timeframe, event: EventType,  payload: Any):
        self.notify( timeframe, event, **payload)

    def _on_trend(self, timeframe:Timeframe, event: EventType,  payload: Any):
        self.notify( timeframe, event, **payload)

    # ------- 内部方法 -------
    def _update_keylevels(self, period: str, cbar):
        # 例如记录 EMA、波段高低点、关键价位等
        raise NotImplementedError()

    # ------- 最高层接口（给 keyzone / supply / decision） -------
    def get_context(self, trading_period: str):
        """
        返回：一个“快照”结构，包含交易周期及其上下级周期的结构信息
        """
        raise NotImplementedError()
