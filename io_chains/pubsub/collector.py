from asyncio import Queue
from logging import getLogger
from typing import Any

from io_chains.pubsub.sentinel import EndOfStream
from io_chains.pubsub.subscriber import Subscriber
from io_chains.utils.converters import iter_over_async

logger = getLogger(__name__)


class Collector(Subscriber):
    """
    Subscriber that buffers items in a queue for iteration.

    Supports both async and sync iteration protocols directly:
        async for item in collector: ...
        for item in collector: ...
    """

    def __init__(self):
        self._queue: Queue = Queue()

    def push(self, datum: Any) -> None:
        try:
            self._queue.put_nowait(datum)
        except Exception as e:
            logger.exception(f"Collector.push: {e}")

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        next_item = await self._queue.get()
        if isinstance(next_item, EndOfStream):
            raise StopAsyncIteration
        return next_item

    def __iter__(self):
        async def _async_gen():
            async for item in self:
                yield item
        yield from iter_over_async(_async_gen())
