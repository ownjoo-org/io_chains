from asyncio import iscoroutinefunction
from logging import getLogger
from queue import Queue
from typing import Any, Callable, Iterable, Optional, Union

from io_chains.linkables.linkable import Linkable
from io_chains.subscribables.consts import MAX_QUEUE_SIZE
from io_chains.subscribables.publisher import Publisher
from io_chains.subscribables.subscriber import Subscriber

logger = getLogger(__name__)


class Link(Linkable, Publisher, Subscriber):
    def __init__(
        self,
        *args,
        in_iter: Union[Callable, Iterable, None] = None,
        processor: Optional[Callable] = None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[Callable, Iterable, None] = None
        self.input = in_iter

        self._processor: Optional[Callable] = None
        self.processor = processor
        self._processing: bool = True

        self._queue: Queue = Queue(maxsize=MAX_QUEUE_SIZE)

    @property
    def input(self) -> Any:
        if isinstance(self._input, Callable):
            return self._input()
        else:
            return self._input

    @input.setter
    def input(self, in_obj: Union[Callable, Iterable, None] = None) -> None:
        if in_obj:
            if not isinstance(in_obj, (Callable, Iterable)):
                raise TypeError(f'in_iter must be Callable or Iterable, got {type(in_obj)}')
        self._input = in_obj

    @property
    def processor(self) -> Any:
        return self._processor

    @processor.setter
    def processor(self, processor: Optional[Callable] = None) -> None:
        self._processor = processor

    async def _fill_queue_from_input(self) -> None:
        if self.input:
            while not self._queue.full():
                try:
                    next_item = next(self.input)
                    if self.processor and isinstance(self.processor, Callable):
                        if iscoroutinefunction(self.processor):
                            next_item = await self.processor(next_item)
                        else:
                            next_item = self.processor(next_item)
                    self._queue.put(next_item)
                except StopIteration:
                    raise

    async def _update_subscribers(self) -> None:
        while not self._queue.empty():
            message = self._queue.get()
            await self.publish(message)

    async def push(self, message: Any) -> None:
        if self._processor and isinstance(self._processor, Callable):
            if iscoroutinefunction(self.processor):
                message = await self.processor(message)
            else:
                message = self.processor(message)
        self._queue.put(message)
        await self._update_subscribers()

    async def __call__(self) -> None:
        # TODO: This is wrong. ExtractLink's functionality can be absorbed into this, yes?
        while True:
            try:
                await self._fill_queue_from_input()
            except Exception:
                break
            finally:
                await self._update_subscribers()
