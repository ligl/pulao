from typing import Callable, Any, List

from pulao.constant import EventType, Const

Subscriber = Callable[[EventType, Any], None]


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

    def notify(self, event_type: EventType, payload: Any = None) -> None:
        for fn in list(self._subscribers):
            if Const.DEBUG:
                fn(event_type, payload)
            else:
                try:
                    fn(event_type, payload)
                except Exception:
                    import traceback

                    traceback.print_exc()
