from queue import Queue
from typing import Any, Callable, Iterable, Optional, Union

from linkables.linkable import Linkable
from subscribables.consts import MAX_QUEUE_SIZE
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
            while not self._queue.full():
                try:
                    if self._processor and isinstance(self._processor, Callable):
                        self._queue.put(self._processor(next(self.input)))
                    else:
                        self._queue.put(next(self.input))
                except StopIteration:
                    raise

    def _update_subscribers(self) -> None:
        while not self._queue.empty():
            message = self._queue.get()
            self.publish(message)

    def push(self, message: Any) -> None:
        if self._processor and isinstance(self._processor, Callable):
            self._queue.put(self._processor(message))
        else:
            self._queue.put(message)
        self._update_subscribers()

    def __call__(self) -> None:
        # TODO: This is wrong. ExtractLink's functionality can be absorbed into this, yes?
        while True:
            try:
                self._fill_queue_from_input()
            except Exception:
                break
            finally:
                self._update_subscribers()
