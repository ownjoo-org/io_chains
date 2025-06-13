from abc import ABC
from typing import Callable, Iterable, Union, Any

from io_chains.subscribables.subscriber import Subscriber


class Publisher(ABC):
    def __init__(
        self,
        *args,
        subscribers: Union[Iterable[Callable], None, Callable] = None,
        **kwargs
    ) -> None:
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Callable | Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(self, subscribers: Union[Iterable[Callable], None, Callable]) -> None:
        if not subscribers:
            return
        if isinstance(subscribers, (Callable, Subscriber)):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if isinstance(subscriber, (Callable, Subscriber)):
                    self._subscribers.append(subscriber)
        else:
            raise TypeError('subscribers must be a Subscriber or Iterable[Subscriber]')

    async def publish(self, datum) -> Any:
        for subscriber in self.subscribers:
            if isinstance(subscriber, Subscriber):
                return await subscriber.push(datum),
            elif isinstance(subscriber, Callable):
                return subscriber(datum),
            else:
                raise TypeError('subscriber must be directly Callable or Subscriber')
