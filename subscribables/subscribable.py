from abc import abstractmethod
from typing import Any


class Subscribable:
    @abstractmethod
    def push(self, value: Any) -> None:
        raise NotImplementedError
