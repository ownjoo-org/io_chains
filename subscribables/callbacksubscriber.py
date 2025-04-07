from sys import stderr
from typing import Any, Callable

from subscribables.subscriber import Subscriber


class CallbackSubscriber(Subscriber):
    def __init__(self, callback: Callable):
        self._callback = callback

    def push(self, message: Any) -> None:
        try:
            self._callback(message)
        except Exception as e:
            print(f"Exception calling {self._callback}: {e}", file=stderr)
