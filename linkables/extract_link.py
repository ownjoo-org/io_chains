from typing import Optional, Iterable

from linkables.link import Link


class ExtractLink(Link):
    def _publish(self) -> None:
        if self._subscribers:
            for each in self.input:
                for subscriber in self._subscribers:
                    subscriber.push(each)

    def __call__(self) -> Optional[Iterable]:
        """
        Can be used to update subscribers from Generator OR can return a Generator, but not both.
        Processor must be a Generator.
        :return: Optional[Generator, None, None]
        """
        if self._subscribers:
            self._publish()
        else:
            return self.input
