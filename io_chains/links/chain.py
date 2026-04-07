from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Callable, Iterable
from typing import Any

from io_chains.links.linkable import Linkable


def _output_of(linkable: Linkable) -> Linkable:
    """Return the linkable that publishes output: for a Chain, its last link; for a Link, itself."""
    if isinstance(linkable, Chain):
        return _output_of(linkable._links[-1])
    return linkable


def _input_of(linkable: Linkable) -> Linkable:
    """Return the linkable that receives pushed input: for a Chain, its first link; for a Link, itself."""
    if isinstance(linkable, Chain):
        return _input_of(linkable._links[0])
    return linkable


class Chain(Linkable):
    """
    An ordered sequence of Linkables that runs as a single unit.

    Chain is the orchestrator: it wires its internal links together,
    manages their concurrent execution, and presents itself as a single
    Linkable to the outside world.

    From the outside a Chain looks like any other Linkable:
      - Accepts input via source= or push() (routed to the first link)
      - Publishes output through its last link's subscribers
      - await chain() runs the whole pipeline; no external gather() needed

    A Chain's elements can be Links or other Chains.
    """

    def __init__(
        self,
        *args,
        links: list[Linkable],
        source: Callable | Iterable | None = None,
        subscribers: Iterable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._links: list[Linkable] = links

        # Wire each linkable's output to the next linkable's input
        for i in range(len(links) - 1):
            _output_of(links[i]).subscribers = [links[i + 1]]

        if source is not None:
            _input_of(links[0]).input = source

        if subscribers is not None:
            _output_of(links[-1]).subscribers = subscribers

    # --- Linkable interface ---

    @property
    async def input(self) -> AsyncGenerator:
        """Delegates to the first link's input."""
        async for datum in _input_of(self._links[0]).input:
            yield datum

    @input.setter
    def input(self, in_iter) -> None:
        """Route input assignment to the first link."""
        _input_of(self._links[0]).input = in_iter

    async def push(self, datum: Any) -> None:
        """Route pushed data to the first link."""
        await _input_of(self._links[0]).push(datum)

    async def run(self) -> None:
        """Run all internal links concurrently under a TaskGroup.
        Callers just await chain() — no external gather() or create_task() needed."""
        async with TaskGroup() as tg:
            for link in self._links:
                tg.create_task(link())

    async def __call__(self) -> None:
        await self.run()
