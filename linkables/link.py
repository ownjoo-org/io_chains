from queue import Queue
from typing import Any, Callable, Iterable, Iterator, Optional, Union

from linkables.linkable import END_OF_QUEUE, Linkable
from linkables.subscriber import Subscriber


class Link(Linkable):
    def __init__(
            self,
            in_iter: Iterable = None,
            processor: Optional[Callable] = None,
            subscribers: Union[Iterable[Subscriber], None, Subscriber] = None,
    ) -> None:
        self._input: Iterator = iter(in_iter) if in_iter else None
        self._queue: Queue = Queue(maxsize=100)
        self._processor: Optional[Callable] = processor
        self._processing: bool = True
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def input(self) -> Any:
        return self._input

    @input.setter
    def input(self, in_obj: Optional[Iterable] = None) -> None:
        self._input = in_obj

    @property
    def processor(self) -> Any:
        return self._processor

    @processor.setter
    def processor(self, processor: Optional[Iterable] = None) -> None:
        self._processor = processor

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(self, subscribers: Union[Iterable[Subscriber], None, Subscriber]) -> None:
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

    def _fill_queue_from_input(self) -> None:
        if self._input:
            while self._processing and not self._queue.full():
                try:
                    if self._processor and isinstance(self._processor, Callable):
                        self._queue.put(self._processor(next(self._input)))
                    else:
                        self._queue.put(next(self._input))
                except StopIteration:
                    self._processing = False

    def push(self, value: Any) -> None:
        if value is not END_OF_QUEUE:
            if self._processor and isinstance(self._processor, Callable):
                self._queue.put(self._processor(value))
            else:
                self._queue.put(value)
            self._publish()
        else:
            self._processing = False

    def __call__(self) -> None:
        while self._processing:
            self._fill_queue_from_input()
            self._publish()
