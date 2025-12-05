from typing import Callable, Any, List, Union, Dict
from collections import defaultdict

from pulao.constant import EventType, Timeframe

Subscriber = Callable[[Timeframe, EventType, Any], None]

class Observable:
    ALL = "ALL_EVENTS"  # 通配符，表示订阅所有事件

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Subscriber]] = defaultdict(list)

    def _ensure_list(self, event_type: Union[EventType, List[EventType], None]) -> Union[List[EventType], None]:
        if event_type is None:
            return None
        if isinstance(event_type, list):
            return event_type
        return [event_type]  # 单个事件类型转为列表

    def subscribe(self, fn: Subscriber, event_type: Union[EventType, List[EventType], None] = None) -> None:
        """
        订阅一个或多个事件
        event_type=None 表示订阅所有事件
        """
        event_type = self._ensure_list(event_type)
        if event_type is None:
            if fn not in self._subscribers[self.ALL]:
                self._subscribers[self.ALL].append(fn)
        else:
            for et in event_type:
                if fn not in self._subscribers[et]:
                    self._subscribers[et].append(fn)

    def unsubscribe(self, fn: Subscriber, event_type: Union[EventType, List[EventType], None] = None) -> None:
        """
        取消订阅一个或多个事件
        event_type=None 表示取消所有事件订阅
        """
        event_type = self._ensure_list(event_type)
        if event_type is None:
            for subs in self._subscribers.values():
                if fn in subs:
                    subs.remove(fn)
        else:
            for et in event_type:
                subs = self._subscribers.get(et)
                if subs and fn in subs:
                    subs.remove(fn)
        # 清理空列表
        self._subscribers = {k: v for k, v in self._subscribers.items() if v}

    def notify(self, timeframe: Timeframe, event_type: EventType, **kwargs) -> None:
        # 先通知订阅了当前事件的订阅者
        for fn in self._subscribers.get(event_type, []):
            fn(timeframe, event_type, kwargs)
        # 再通知订阅所有事件的订阅者
        for fn in self._subscribers.get(self.ALL, []):
            fn(timeframe, event_type, kwargs)
