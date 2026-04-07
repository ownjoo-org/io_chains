from asyncio import Queue, QueueEmpty
from logging import getLogger
from typing import Any

from io_chains.pubsub.sentinel import EndOfStream
from io_chains.pubsub.subscriber import Subscriber

logger = getLogger(__name__)


class Collector(Subscriber):
    """
    Subscriber that buffers items in a queue for iteration.

    Supports both async and sync iteration:
        async for item in collector: ...   (preferred; works inside a running event loop)
        for item in collector: ...         (works after the pipeline has completed)
    """

    def __init__(self, maxsize: int = 0):
        self._queue: Queue = Queue(maxsize=maxsize)

    async def push(self, datum: Any) -> None:
        await self._queue.put(datum)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        item = await self._queue.get()
        if isinstance(item, EndOfStream):
            raise StopAsyncIteration
        return item

    def __iter__(self):
        """Drain buffered items synchronously. Only use after the pipeline has completed."""
        while True:
            try:
                item = self._queue.get_nowait()
            except QueueEmpty:
                break
            if isinstance(item, EndOfStream):
                break
            yield item
