from typing import List, Any
from pulao.events import Observable
from .trend import Trend
from ..constant import EventType
from ..swing import SwingManager, Swing


class TrendManager(Observable):
    swing_manager : SwingManager

    def __init__(self, swing_manager: SwingManager):
        super().__init__()
        self.swing_manager = swing_manager
        self.swing_manager.subscribe(self._on_swing_changed)

    def _on_swing_changed(self, event: EventType, payload: Any):
        self.detect()

    def detect(self):
        """
        趋势检测识别
        """

    def add(self, trend: Trend):
        self.notify(EventType.TREND_CHANGED, trend)


