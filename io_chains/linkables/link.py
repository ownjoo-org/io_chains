from asyncio import Queue, Task, create_task, gather, iscoroutinefunction
from inspect import isfunction
from logging import getLogger
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Iterable, Optional, Union

from io_chains.linkables.linkable import Linkable

logger = getLogger(__name__)


class Link(Linkable):
    def __init__(
        self,
        *args,
        in_iter: Union[Callable, Iterable, None] = None,
        transformer: Optional[Callable] = None,
        queue: Optional[Queue] = None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[AsyncIterable, Callable, Iterable, None] = None
        self.input = in_iter

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
                raise TypeError(f'in_iter must be Callable or Iterable, got {type(in_obj)}')
        self._input = in_obj

    @property
    def transformer(self) -> Any:
        return self._transformer

    @transformer.setter
    def transformer(self, transformer: Optional[Callable] = None) -> None:
        self._transformer = transformer

    async def _fill_queue_from_input(self) -> None:
        if self.input:
            try:
                async for datum in self.input:
                    self.push(datum)
                self.push(None)
            except (StopIteration, StopAsyncIteration) as exc_stop:
                logger.warning(f'STOPPING FILL FROM INPUT: {exc_stop}')
                raise

    async def _update_subscribers(self) -> None:
        should_continue: bool = True
        while should_continue:
            datum: Any = await self._queue.get()
            await self.publish(datum)
            should_continue = datum is not None

    def push(self, datum: Any) -> None:
        if self._transformer and isinstance(self._transformer, Callable):
            if datum is not None:
                datum = self.transformer(datum)
        self._queue.put_nowait(datum)

    async def run(self) -> None:
        tasks: list[Task] = [
            create_task(self._fill_queue_from_input()),
            create_task(self._update_subscribers()),
        ]
        await gather(*tasks)

    async def __call__(self) -> Optional[tuple]:
        return await self.run()
