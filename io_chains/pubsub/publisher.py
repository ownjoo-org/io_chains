from abc import ABC
from asyncio import TaskGroup
from collections.abc import Iterable
from typing import Any, Union

from io_chains.pubsub.subscriber import Subscriber


class Publisher(ABC):
    def __init__(
        self,
        *args,
        subscribers: Union['Subscriber', Iterable['Subscriber'], None] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._subscribers: list[Subscriber] = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(self, subscribers: Union['Subscriber', Iterable['Subscriber'], None]) -> None:
        if not subscribers:
            return
        if isinstance(subscribers, Subscriber):
            self._subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if not isinstance(subscriber, Subscriber):
                    raise TypeError(f'each subscriber must be a Subscriber, got {type(subscriber)}')
                self._subscribers.append(subscriber)
        else:
            raise TypeError('subscribers must be a Subscriber or Iterable[Subscriber]')

    async def publish(self, datum: Any) -> None:
        if not self._subscribers:
            return
        if len(self._subscribers) == 1:
            await self._subscribers[0].push(datum)
        else:
            async with TaskGroup() as tg:
                for subscriber in self._subscribers:
                    tg.create_task(subscriber.push(datum))
