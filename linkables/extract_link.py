from typing import Optional, Iterable, Callable

from linkables.link import Link
from subscribables.subscriber import Subscriber


class ExtractLink(Link):
    def _publish(self) -> None:
        if self.subscribers:
            for each in self.input:
                self.publish(each)

    def __call__(self) -> Optional[Iterable]:
        """
        Can be used to update subscribers from Generator OR can return a Generator, but not both.
        Processor must be a Generator.
        :return: Optional[Generator, None, None]
        """
        if self.subscribers:
            self._publish()
        else:
            return self.input
