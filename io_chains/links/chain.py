from asyncio import create_task, gather
from typing import Any, AsyncGenerator, Callable, Iterable, Optional, Union

from io_chains.links.linkable import Linkable
from io_chains.pubsub.sentinel import EndOfStream


def _output_of(linkable: Linkable) -> Linkable:
    """Return the linkable that actually publishes output for this linkable.
    For a Chain, that's its last internal link. For a Link, it's itself."""
    if isinstance(linkable, Chain):
        return _output_of(linkable._links[-1])
    return linkable


def _input_of(linkable: Linkable) -> Linkable:
    """Return the linkable that receives pushed input for this linkable.
    For a Chain, that's its first internal link. For a Link, it's itself."""
    if isinstance(linkable, Chain):
        return _input_of(linkable._links[0])
    return linkable


class Chain(Linkable):
    """
    An ordered sequence of Linkables that runs as a single unit.

    Chain is the orchestrator: it wires its internal links together,
    manages their concurrent execution, and presents itself as a single
    Linkable to the outside world.

    From the outside, a Chain looks like any other Linkable:
    - It accepts input via in_iter or push() (routed to the first link)
    - It publishes output through its last link's subscribers
    - await chain() runs the whole pipeline; no external gather() needed

    A Chain's elements can be Links or other Chains.
    """

    def __init__(
        self,
        *args,
        links: list[Linkable],
        source: Union[Callable, Iterable, None] = None,
        subscribers: Union[Iterable[Callable], Callable, None] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._links: list[Linkable] = links

        # Wire each linkable's output to the next linkable's input
        for i in range(len(links) - 1):
            _output_of(links[i]).subscribers = [links[i + 1]]

        # Attach external input to the first link
        if source is not None:
            _input_of(links[0]).input = source

        # Attach external subscribers to the last link's output
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

    def push(self, datum: Any) -> None:
        """Route pushed data to the first link."""
        _input_of(self._links[0]).push(datum)

    async def _fill_queue_from_input(self) -> None:
        """Chain delegates input filling to its first link."""
        pass

    async def run(self) -> None:
        """Gather all internal links concurrently. This is the key Chain promise:
        callers just await chain() — no external gather() or create_task() needed."""
        await gather(*[create_task(link()) for link in self._links])

    async def __call__(self) -> None:
        return await self.run()
