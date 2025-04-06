from sys import stderr
from typing import Any, Callable

from subscribables.subscribable import Subscribable


class Subscriber(Subscribable):
    def __init__(self, callback: Callable):
        self._callback = callback

    def push(self, value: Any) -> None:
        try:
            self._callback(value)
        except Exception as e:
            print(f"Exception calling {self._callback}: {e}", file=stderr)
