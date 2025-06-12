from abc import abstractmethod, ABC
from typing import Any


class Subscriber(ABC):
    @abstractmethod
    async def push(self, datum: Any) -> Any:
        pass
