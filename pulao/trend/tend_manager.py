from typing import List
from pulao.events import Observable
from .trend import Trend
from ..constant import EventType


class TrendManager(Observable):
    trends: List[Trend]

    def __init__(self):
        super().__init__()
        self.trends = []

    def add(self, trend: Trend):
        self.trends.append(trend)
        self.notify(EventType.TREND_CHANGED, trend)

