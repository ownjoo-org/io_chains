from abc import abstractmethod, ABC
from typing import Any


class Subscriber(ABC):
    @abstractmethod
    def push(self, datum: Any) -> Any:
        pass
