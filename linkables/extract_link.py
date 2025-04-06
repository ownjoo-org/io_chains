from typing import Any, Generator, Optional, Union, Iterable, Callable

from linkables.link import Link


class ExtractLink(Link):
    def __init__(self, *args, in_iter: Union[Callable, Iterable], **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._in_iter = in_iter

    def _get_iter(self) -> Iterable:
        if isinstance(self._in_iter, Callable):
            return self._in_iter()
        elif isinstance(self._in_iter, Iterable):
            return self._in_iter
        else:
            raise TypeError(f'in_iter must be Callable or Iterable, got {type(self._in_iter)}')

    def _publish(self) -> None:
        if self._subscribers:
            for each in self._get_iter():
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
            return self._get_iter()
