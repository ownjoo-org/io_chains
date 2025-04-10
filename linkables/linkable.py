from abc import abstractmethod
from typing import Any

from subscribables.publisher import Publisher
from subscribables.subscriber import Subscriber


class Linkable(Publisher, Subscriber):
    @abstractmethod
    def input(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    @abstractmethod
    def processor(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _fill_queue_from_input(self, *args, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    def __call__(self, *args, **kwargs) -> None:
        raise NotImplementedError
