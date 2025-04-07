from typing import Iterable, Optional

from linkables.link import Link


class ExtractLink(Link):
    def _publish(self) -> None:
        for each in self.input:
            self.publish(each)

    def __call__(self) -> Optional[Iterable]:
        """
        Can be used to update subscribers from Generator OR can return a Generator, but not both.
        :return: Optional[Generator, None, None]
        """
        if self.subscribers:
            self._publish()
        else:
            return self.input
