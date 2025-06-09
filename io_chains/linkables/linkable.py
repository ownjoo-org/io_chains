from abc import abstractmethod
from typing import Any

from io_chains.subscribables.publisher import Publisher
from io_chains.subscribables.subscriber import Subscriber


class Linkable(Publisher, Subscriber):
    @abstractmethod
    async def input(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def processor(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def _fill_queue_from_input(self, *args, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self, *args, **kwargs) -> None:
        raise NotImplementedError
