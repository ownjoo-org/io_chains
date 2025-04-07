from queue import Queue
from typing import Any, Callable, Iterable, Optional, Union

from linkables.linkable import Linkable
from subscribables.consts import END_OF_QUEUE, MAX_QUEUE_SIZE
from subscribables.publisher import Publisher
from subscribables.subscriber import Subscriber


class Link(Linkable, Publisher, Subscriber):
    def __init__(
        self,
        *args,
        in_iter: Union[Callable, Iterable, None] = None,
        processor: Optional[Callable] = None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: Union[Callable, Iterable, None] = None
        self.input = in_iter

        self._processor: Optional[Callable] = None
        self.processor = processor
        self._processing: bool = True

        self._queue: Queue = Queue(maxsize=MAX_QUEUE_SIZE)

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
                    self._queue.put(END_OF_QUEUE)
                    self._processing = False

    def _publish(self) -> None:
        while not self._queue.empty():
            message = self._queue.get()
            self.publish(message)

    def push(self, value: Any) -> None:
        if value is END_OF_QUEUE:
            self._processing = False
        if self._processor and isinstance(self._processor, Callable):
            self._queue.put(self._processor(value))
        else:
            self._queue.put(value)
        self._publish()

    def __call__(self) -> None:
        while self._processing:
            self._fill_queue_from_input()
            self._publish()
