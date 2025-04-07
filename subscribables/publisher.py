from abc import ABC
from queue import Queue
from typing import Iterable, Union

from subscribables.consts import MAX_QUEUE_SIZE
from subscribables.subscriber import Subscriber


class Publisher(ABC):
    def __init__(
        self,
        *args,
        subscribers: Union[Iterable[Subscriber], None, Subscriber] = None,
        **kwargs
    ) -> None:
        self._queue: Queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(
        self, subscribers: Union[Iterable[Subscriber], None, Subscriber]
    ) -> None:
        if isinstance(subscribers, Subscriber):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if isinstance(subscriber, Subscriber):
                    self._subscribers.append(subscriber)

    def _publish(self) -> None:
        while self._subscribers and not self._queue.empty():
            message = self._queue.get()
            for subscriber in self._subscribers:
                subscriber.push(message)
