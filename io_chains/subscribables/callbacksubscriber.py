from logging import getLogger
from typing import Any, Callable

from io_chains.subscribables.subscriber import Subscriber

logger = getLogger(__name__)


class CallbackSubscriber(Subscriber):
    def __init__(self, callback: Callable):
        self._callback = callback

    async def push(self, message: Any) -> None:
        try:
            self._callback(message)
        except Exception as e:
            logger.exception(f"CallbackSubscriber.push: {self._callback}: {e}")
