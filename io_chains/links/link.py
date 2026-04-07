from asyncio import Lock, Queue, TaskGroup
from inspect import isawaitable, isgenerator
from logging import getLogger
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Iterable, Optional, Union

from io_chains.links.linkable import Linkable
from io_chains.pubsub.sentinel import END_OF_STREAM, EndOfStream, SKIP, Skip

logger = getLogger(__name__)


class Link(Linkable):
    def __init__(
        self,
        *args,
        source: Union[Callable, Iterable, None] = None,
        transformer: Optional[Callable] = None,
        queue_size: int = 0,
        workers: int = 1,
        upstream_count: int = 1,
        batch_size: int = 1,
        on_error: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[AsyncIterable, Callable, Iterable, None] = None
        self.input = source

        self._transformer: Optional[Callable] = None
        self.transformer = transformer

        self._queue: Queue = Queue(maxsize=queue_size)
        self._workers: int = max(1, workers)
        self._active_workers: int = self._workers
        self._upstream_count: int = max(1, upstream_count)
        self._eos_received: int = 0
        self._batch_size: int = max(1, batch_size)
        self._on_error: Optional[Callable] = on_error

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

    async def _collect_batch(self) -> tuple[list, bool]:
        """Collect up to batch_size items. Returns (batch, eos_received)."""
        batch = []
        while len(batch) < self._batch_size:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                return batch, True
            batch.append(datum)
        return batch, False

    async def _process_and_publish(self, datum: Any) -> None:
        """Transform datum (or batch list) and publish result(s)."""
        if self._transformer:
            try:
                result = self._transformer(datum)
                if isawaitable(result):
                    result = await result
            except Exception as e:
                if self._on_error is not None:
                    result = self._on_error(e, datum)
                    if isawaitable(result):
                        result = await result
                else:
                    raise

            if isinstance(result, Skip):
                return
            elif isgenerator(result):
                for item in result:
                    await self.publish(item)
                return
            elif hasattr(result, '__aiter__'):
                async for item in result:
                    await self.publish(item)
                return
            datum = result
        elif self._batch_size > 1 and isinstance(datum, list):
            # No transformer in batch mode: publish each item individually
            for item in datum:
                await self.publish(item)
            return

        await self.publish(datum)

    async def _handle_eos(self, lock: Lock) -> None:
        async with lock:
            self._active_workers -= 1
            if self._active_workers == 0:
                await self.publish(END_OF_STREAM)

    async def _worker(self, lock: Lock) -> None:
        while True:
            if self._batch_size > 1:
                batch, eos_received = await self._collect_batch()
                if batch:
                    await self._process_and_publish(batch)
                if eos_received:
                    await self._handle_eos(lock)
                    break
            else:
                datum = await self._queue.get()
                if isinstance(datum, EndOfStream):
                    await self._handle_eos(lock)
                    break
                await self._process_and_publish(datum)

    async def push(self, datum: Any) -> None:
        if isinstance(datum, EndOfStream):
            self._eos_received += 1
            if self._eos_received < self._upstream_count:
                return  # still waiting for other upstreams
            for _ in range(self._workers):
                await self._queue.put(datum)
        else:
            await self._queue.put(datum)

    async def run(self) -> None:
        self._active_workers = self._workers
        self._eos_received = 0
        lock = Lock()
        async with TaskGroup() as tg:
            tg.create_task(self._fill_queue_from_input())
            for _ in range(self._workers):
                tg.create_task(self._worker(lock))

    async def __call__(self) -> None:
        await self.run()
