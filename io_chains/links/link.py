from asyncio import Queue, Task, create_task, gather
from inspect import isfunction, isawaitable
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
        queue: Optional[Queue] = None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[AsyncIterable, Callable, Iterable, None] = None
        self.input = source

        self._transformer: Optional[Callable] = None
        self.transformer = transformer
        self._transforming: bool = True

        self._queue: Queue = queue or Queue()

    @property
    async def input(self) -> AsyncGenerator:
        if self._input:
            if hasattr(self._input, '__aiter__'):
                async for each in self._input:
                    yield each
            elif isfunction(self._input):
                async for each in self._input():
                    yield each
            else:
                for each in self._input:
                    yield each

    @input.setter
    def input(self, in_obj: Union[AsyncIterable, Callable, Iterable, None] = None) -> None:
        if in_obj:
            if not isinstance(in_obj, (AsyncIterable, Callable, Iterable)):
                raise TypeError(f'source must be Callable or Iterable, got {type(in_obj)}')
        self._input = in_obj

    @property
    def transformer(self) -> Any:
        return self._transformer

    @transformer.setter
    def transformer(self, transformer: Optional[Callable] = None) -> None:
        self._transformer = transformer

    async def _fill_queue_from_input(self) -> None:
        if self._input:
            try:
                async for datum in self.input:
                    self.push(datum)
                self.push(END_OF_STREAM)
            except (StopIteration, StopAsyncIteration) as exc_stop:
                logger.warning(f'STOPPING FILL FROM INPUT: {exc_stop}')
                raise

    async def _update_subscribers(self) -> None:
        should_continue: bool = True
        while should_continue:
            datum: Any = await self._queue.get()
            if not isinstance(datum, EndOfStream) and self._transformer:
                result = self._transformer(datum)
                datum = await result if isawaitable(result) else result
            await self.publish(datum)
            should_continue = not isinstance(datum, EndOfStream)

    def push(self, datum: Any) -> None:
        self._queue.put_nowait(datum)

    async def run(self) -> None:
        tasks: list[Task] = [
            create_task(self._fill_queue_from_input()),
            create_task(self._update_subscribers()),
        ]
        await gather(*tasks)

    async def __call__(self) -> Optional[tuple]:
        return await self.run()
