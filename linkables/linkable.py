from abc import abstractmethod
from typing import Any

from subscribables.publisher import Publisher
from subscribables.subscriber import Subscriber


class Linkable(Publisher, Subscriber):
    @abstractmethod
    def input(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def processor(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _fill_queue_from_input(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def __call__(self) -> None:
        raise NotImplementedError
