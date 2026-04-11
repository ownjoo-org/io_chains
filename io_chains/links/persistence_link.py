from collections.abc import AsyncIterable, Callable, Iterable
from inspect import isawaitable
from typing import Any

from oj_persistence.store.async_base import AsyncAbstractStore

from io_chains._internal.link import Link
from io_chains._internal.sentinel import END_OF_STREAM, EndOfStream

_OPERATIONS = {"upsert", "create", "update"}


class PersistenceLink(Link):
    """
    A mid-chain tap: writes each item to an async store as a side effect,
    then passes the item through to downstream subscribers unchanged.

    The store's async context manager is entered at the start of run() and
    exited on completion, guaranteeing any buffered writes are flushed.

    Parameters
    ----------
    store:      Any AsyncAbstractStore implementation.
    key_fn:     Callable that extracts the store key (str) from each item.
    operation:  'upsert' (default) | 'create' | 'update'
    """

    def __init__(
        self,
        *args,
        store: AsyncAbstractStore,
        key_fn: Callable[[Any], str],
        operation: str = "upsert",
        source: Callable | Iterable | None = None,
        **kwargs,
    ) -> None:
        if operation not in _OPERATIONS:
            raise ValueError(f"operation must be one of {_OPERATIONS}, got {operation!r}")
        super().__init__(*args, **kwargs)
        self._store = store
        self._key_fn = key_fn
        self._operation = operation
        self._input: AsyncIterable | Callable | Iterable | None = None
        self.input = source

    @property
    async def input(self):
        if self._input is None:
            return
        src = self._input() if callable(self._input) else self._input
        if hasattr(src, "__aiter__"):
            async for each in src:
                yield each
        else:
            for each in src:
                yield each

    @input.setter
    def input(self, in_obj) -> None:
        self._input = in_obj

    async def _fill_queue_from_input(self) -> None:
        if self._input is not None:
            async for datum in self.input:
                await self.push(datum)
            await self.push(END_OF_STREAM)

    async def run(self) -> None:
        self._reset_metrics()
        async with self._store:
            from asyncio import TaskGroup
            try:
                async with TaskGroup() as tg:
                    tg.create_task(self._fill_queue_from_input())
                    tg.create_task(self._process_loop())
            except ExceptionGroup as eg:
                if len(eg.exceptions) == 1:
                    raise eg.exceptions[0]
                raise
        await self._emit_metrics()

    async def _process_loop(self) -> None:
        write = getattr(self._store, self._operation)
        while True:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                await self.publish(END_OF_STREAM)
                break
            try:
                result = write(self._key_fn(datum), datum)
                if isawaitable(result):
                    await result
            except Exception as e:
                self._items_errored += 1
                if self._on_error is not None:
                    handler = self._on_error(e, datum)
                    if isawaitable(handler):
                        await handler
                else:
                    raise
            await self.publish(datum)
