from abc import abstractmethod
from typing import Any

from io_chains.pubsub.publisher import Publisher
from io_chains.pubsub.subscriber import Subscriber


class Linkable(Publisher, Subscriber):
    """
    Combines Publisher and Subscriber: a Linkable can receive data (push)
    and emit data (publish) — it sits in the middle of a pipeline.

    Concrete subclasses must implement:
      - input property (async generator over the source)
      - _fill_queue_from_input (drive the source into the internal queue)
      - push (satisfy Subscriber; route datum into the internal queue)
      - __call__ (run the linkable)
    """

    @property
    @abstractmethod
    async def input(self) -> Any:
        pass

    @abstractmethod
    async def _fill_queue_from_input(self) -> None:
        pass

    @abstractmethod
    async def __call__(self) -> None:
        pass
