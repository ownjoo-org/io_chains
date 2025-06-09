from abc import abstractmethod
from typing import Any


class Subscriber:
    @abstractmethod
    async def push(self, message: Any) -> None:
        raise NotImplementedError

    async def __call__(self, value: Any) -> None:
        await self.push(message=value)
