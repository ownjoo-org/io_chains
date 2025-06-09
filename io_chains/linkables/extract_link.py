from typing import Iterable, Optional

from io_chains.linkables.link import Link


class ExtractLink(Link):
    def _update_subscribers(self) -> None:
        for each in self.input:
            self.publish(each)

    def __call__(self) -> Optional[Iterable]:
        """
        Can be used to update subscribers from Generator OR can return a Generator, but not both.
        :return: Optional[Generator, None, None]
        """
        if self.subscribers:
            self._update_subscribers()
        else:
            return self.input
