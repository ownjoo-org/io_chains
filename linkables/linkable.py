from abc import ABC, abstractmethod
from typing import Any

from linkables.subscriber import Subscriber


class Linkable(ABC, Subscriber):
    @abstractmethod
    def input(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def processor(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def subscribers(self) -> list[Subscriber]:
        raise NotImplementedError

    @abstractmethod
    def _publish(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _fill_queue_from_input(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def push(self, value: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def __call__(self) -> None:
        raise NotImplementedError
