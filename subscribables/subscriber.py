from abc import abstractmethod
from typing import Any


class Subscriber:
    @abstractmethod
    def push(self, message: Any) -> None:
        raise NotImplementedError

    def __call__(self, value: Any) -> None:
        self.push(message=value)
