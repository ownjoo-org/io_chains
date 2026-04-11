from abc import ABC, abstractmethod
from typing import Any


class Subscriber(ABC):
    @abstractmethod
    async def push(self, datum: Any) -> None:
        pass
