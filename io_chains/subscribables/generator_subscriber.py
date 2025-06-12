from asyncio import Queue
from logging import getLogger
from typing import Any, Generator

from io_chains.subscribables.subscriber import Subscriber

logger = getLogger(__name__)


class GeneratorSubscriber(Subscriber):
    def __init__(self):
        self._queue: Queue = Queue()

    async def push(self, datum: Any) -> None:
        try:
            self._queue.put_nowait(datum)
        except Exception as e:
            logger.exception(f"GeneratorSubscriber.push: {e}")

    async def out(self) -> Generator[Any, Any, Any]:
        should_continue: bool = True
        while should_continue:
            next_item = await self._queue.get()
            if should_continue := next_item is not None:
                yield next_item
