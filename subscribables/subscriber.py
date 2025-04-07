from abc import ABC, abstractmethod
from typing import Any


class Subscriber(ABC):
    @abstractmethod
    def push(self, value: Any) -> None:
        raise NotImplementedError
