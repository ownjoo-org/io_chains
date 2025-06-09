from typing import Iterable, Optional

from io_chains.linkables.link import Link


class ExtractLink(Link):
    async def _update_subscribers(self) -> None:
        if self.input and hasattr(self.input, '__aiter__'):
            async for each in self.input:
                await self.publish(each)
        else:
            for each in self.input:
                await self.publish(each)

    async def __call__(self) -> Optional[Iterable]:
        """
        Can be used to update subscribers from Generator OR can return a Generator, but not both.
        :return: Optional[Generator, None, None]
        """
        if self.subscribers:
            await self._update_subscribers()
        else:
            return self.input
