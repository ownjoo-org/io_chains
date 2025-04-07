from typing import Iterable, Union

from subscribables.subscriber import Subscriber


class Publisher:
    def __init__(
        self,
        *args,
        subscribers: Union[Iterable[Subscriber], None, Subscriber] = None,
        **kwargs
    ) -> None:
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(
        self, subscribers: Union[Iterable[Subscriber], None, Subscriber]
    ) -> None:
        if subscribers is None:
            return
        elif isinstance(subscribers, Subscriber):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if isinstance(subscriber, Subscriber):
                    self._subscribers.append(subscriber)
        else:
            raise TypeError('subscribers must be a Subscriber or Iterable[Subscriber]')

    def publish(self, message) -> None:
        for subscriber in self._subscribers:
            subscriber.push(message)
