from typing import Iterable, Union, Callable

from subscribables.subscriber import Subscriber


class Publisher:
    def __init__(
        self,
        *args,
        subscribers: Union[Iterable[Callable], None, Callable] = None,
        **kwargs
    ) -> None:
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Callable]:
        return self._subscribers

    @subscribers.setter
    def subscribers(
        self, subscribers: Union[Iterable[Callable], None, Callable]
    ) -> None:
        if subscribers is None:
            return
        elif isinstance(subscribers, Callable):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if isinstance(subscriber, Callable):
                    self._subscribers.append(subscriber)
        else:
            raise TypeError('subscribers must be a Subscriber or Iterable[Subscriber]')

    def publish(self, message) -> None:
        for subscriber in self._subscribers:
            if isinstance(subscriber, Subscriber):
                subscriber.push(message)
            elif isinstance(subscriber, Callable):
                subscriber(message)
            else:
                raise TypeError('subscriber must be Callable')
