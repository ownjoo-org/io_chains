from queue import Queue
from typing import Any, Callable, Iterable, Iterator, Optional, Union

from linkables.consts import END_OF_QUEUE
from linkables.linkable import Linkable
from subscribables.subscribable import Subscribable


class Link(Linkable):
    def __init__(
        self,
        *args,
        in_iter: Iterable = None,
        processor: Optional[Callable] = None,
        subscribers: Union[Iterable[Subscribable], None, Subscribable] = None,
        **kwargs
    ) -> None:
        self._input: Union[Callable, Iterator, None] = None
        self.input = in_iter
        self._queue: Queue = Queue(maxsize=100)
        self._processor: Optional[Callable] = processor
        self._processing: bool = True
        self._subscribers: list = []
        self.subscribers = subscribers

    @property
    def input(self) -> Any:
        if isinstance(self._input, Callable):
            return self._input()
        else:
            return self._input

    @input.setter
    def input(self, in_obj: Union[Callable, Iterable, None] = None) -> None:
        if in_obj:
            if not isinstance(in_obj, (Callable, Iterable)):
                raise TypeError(f'in_iter must be Callable or Iterable, got {type(in_obj)}')
        self._input = in_obj

    @property
    def processor(self) -> Any:
        return self._processor

    @processor.setter
    def processor(self, processor: Optional[Iterable] = None) -> None:
        self._processor = processor

    @property
    def subscribers(self) -> list[Subscribable]:
        return self._subscribers

    @subscribers.setter
    def subscribers(
        self, subscribers: Union[Iterable[Subscribable], None, Subscribable]
    ) -> None:
        if isinstance(subscribers, Subscribable):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if isinstance(subscriber, Subscribable):
                    self._subscribers.append(subscriber)

    def _publish(self) -> None:
        while self._subscribers and not self._queue.empty():
            message = self._queue.get()
            for subscriber in self._subscribers:
                subscriber.push(message)

    def _fill_queue_from_input(self) -> None:
        if self.input:
            while self._processing and not self._queue.full():
                try:
                    if self._processor and isinstance(
                        self._processor, Callable
                    ):
                        self._queue.put(self._processor(next(self.input)))
                    else:
                        self._queue.put(next(self.input))
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
