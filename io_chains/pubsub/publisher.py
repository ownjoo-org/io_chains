from asyncio import TaskGroup
from collections.abc import Iterable
from typing import Any

from io_chains.pubsub.subscriber import Subscriber


class Publisher:
    def __init__(
        self,
        *args,
        subscribers: 'Subscriber | Iterable[Subscriber] | None' = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._subscribers: list[Subscriber] = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(self, subscribers: 'Subscriber | Iterable[Subscriber] | None') -> None:
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

    def subscribe(self, subscriber: Subscriber, channel: str | None = None) -> None:
        """Wire a subscriber, optionally tagging each item with a channel label.

        subscribe(sub)               — equivalent to appending sub directly
        subscribe(sub, channel='x')  — wraps each item in Envelope(data, channel='x')
        """
        if channel is not None:
            from io_chains.pubsub.channel_subscriber import ChannelSubscriber
            subscriber = ChannelSubscriber(subscriber=subscriber, channel=channel)
        self._subscribers.append(subscriber)

    async def publish(self, datum: Any) -> None:
        if not self._subscribers:
            return
        if len(self._subscribers) == 1:
            await self._subscribers[0].push(datum)
        else:
            async with TaskGroup() as tg:
                for subscriber in self._subscribers:
                    tg.create_task(subscriber.push(datum))
