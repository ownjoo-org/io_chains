from abc import abstractmethod
from typing import Any


class Subscriber:
    @abstractmethod
    def push(self, value: Any) -> None:
        raise NotImplementedError
