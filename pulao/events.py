from typing import Callable, Any, List

from pulao.constant import EventType, Timeframe

Subscriber = Callable[[EventType,Timeframe, Any], None]


class Observable:
    _subscribers: List[Subscriber]

    def __init__(self) -> None:
        self._subscribers = []

    def subscribe(self, fn: Subscriber) -> None:
        if fn not in self._subscribers:
            self._subscribers.append(fn)

    def unsubscribe(self, fn: Subscriber) -> None:
        if fn in self._subscribers:
            self._subscribers.remove(fn)

    def notify(self, timeframe:Timeframe, event_type: EventType,  **kwargs) -> None:
        for fn in list(self._subscribers):
            fn(event_type, timeframe, kwargs)
