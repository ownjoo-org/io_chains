from inspect import isawaitable
from logging import getLogger
from typing import Any, Callable

from io_chains.pubsub.sentinel import EndOfStream
from io_chains.pubsub.subscriber import Subscriber

logger = getLogger(__name__)


class CallbackSubscriber(Subscriber):
    def __init__(self, callback: Callable):
        self._callback = callback

    async def push(self, datum: Any) -> Any:
        if isinstance(datum, EndOfStream):
            return
        try:
            result = self._callback(datum)
            if isawaitable(result):
                return await result
            return result
        except Exception as e:
            logger.exception(f"CallbackSubscriber.push: {self._callback}: {e}")
