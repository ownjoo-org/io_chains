from abc import abstractmethod
from typing import Any

from io_chains._internal.publisher import Publisher
from io_chains._internal.subscriber import Subscriber


class Linkable(Publisher, Subscriber):
    """
    A link in a data pipeline chain.

    Combines Publisher and Subscriber: a Linkable can receive data (push)
    and emit data (publish) — sitting between upstream and downstream links.

    Concrete subclasses must implement:
      - input property (async generator over the source)
      - push (route datum into the internal queue)
      - __call__ (run the link)
    """

    @property
    @abstractmethod
    async def input(self) -> Any:
        pass

    @abstractmethod
    async def __call__(self) -> None:
        pass
