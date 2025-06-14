from asyncio import Queue
from logging import getLogger
from typing import Any, AsyncGenerator, Generator

from io_chains.subscribables.subscriber import Subscriber
from io_chains.utils.converters import iter_over_async

logger = getLogger(__name__)


class GeneratorSubscriber(Subscriber):
    def __init__(self):
        self._queue: Queue = Queue()

    def push(self, datum: Any) -> None:
        try:
            self._queue.put_nowait(datum)
        except Exception as e:
            logger.exception(f"GeneratorSubscriber.push: {e}")

    async def a_out(self) -> AsyncGenerator[Any, Any]:
        should_continue: bool = True
        while should_continue:
            next_item = await self._queue.get()
            if should_continue := next_item is not None:
                yield next_item

    def out(self) -> Generator[Any, Any, Any]:
        async def generate_from_async_queue() -> Generator[Any, Any, Any]:
            should_continue: bool = True
            while should_continue:
                next_item = await self._queue.get()
                if should_continue := next_item is not None:
                    yield next_item
        yield from iter_over_async(generate_from_async_queue())
