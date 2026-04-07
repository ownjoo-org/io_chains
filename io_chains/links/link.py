from asyncio import Queue, TaskGroup
from inspect import isawaitable
from logging import getLogger
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Iterable, Optional, Union

from io_chains.links.linkable import Linkable
from io_chains.pubsub.sentinel import END_OF_STREAM, EndOfStream

logger = getLogger(__name__)


class Link(Linkable):
    def __init__(
        self,
        *args,
        source: Union[Callable, Iterable, None] = None,
        transformer: Optional[Callable] = None,
        queue_size: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[AsyncIterable, Callable, Iterable, None] = None
        self.input = source

        self._transformer: Optional[Callable] = None
        self.transformer = transformer

        self._queue: Queue = Queue(maxsize=queue_size)

    @property
    async def input(self) -> AsyncGenerator:
        if self._input is None:
            return
        source = self._input() if callable(self._input) else self._input
        if hasattr(source, '__aiter__'):
            async for each in source:
                yield each
        else:
            for each in source:
                yield each

    @input.setter
    def input(self, in_obj: Union[AsyncIterable, Callable, Iterable, None]) -> None:
        if in_obj is not None and not isinstance(in_obj, (AsyncIterable, Callable, Iterable)):
            raise TypeError(f'source must be Callable or Iterable, got {type(in_obj)}')
        self._input = in_obj

    @property
    def transformer(self) -> Optional[Callable]:
        return self._transformer

    @transformer.setter
    def transformer(self, transformer: Optional[Callable]) -> None:
        self._transformer = transformer

    async def _fill_queue_from_input(self) -> None:
        if self._input is not None:
            async for datum in self.input:
                await self.push(datum)
            await self.push(END_OF_STREAM)
        # else: subscriber-only mode — EOS arrives via push() from upstream

    async def _update_subscribers(self) -> None:
        while True:
            datum: Any = await self._queue.get()
            if self._transformer and not isinstance(datum, EndOfStream):
                result = self._transformer(datum)
                datum = await result if isawaitable(result) else result
            await self.publish(datum)
            if isinstance(datum, EndOfStream):
                break

    async def push(self, datum: Any) -> None:
        await self._queue.put(datum)

    async def run(self) -> None:
        async with TaskGroup() as tg:
            tg.create_task(self._fill_queue_from_input())
            tg.create_task(self._update_subscribers())

    async def __call__(self) -> None:
        await self.run()
