from typing import Any

from io_chains._internal.envelope import Envelope
from io_chains._internal.sentinel import EndOfStream
from io_chains._internal.subscriber import Subscriber


class ChannelSubscriber(Subscriber):
    """
    Adapter that wraps each item in an Envelope before forwarding to the real subscriber.
    EndOfStream is forwarded unwrapped so pipeline termination still propagates correctly.
    """

    def __init__(self, subscriber: Subscriber, channel: str) -> None:
        self._subscriber = subscriber
        self._channel = channel
        # If the downstream subscriber tracks upstream count (e.g. Enricher), notify it
        if hasattr(subscriber, "_register_upstream"):
            subscriber._register_upstream()

    async def push(self, datum: Any) -> None:
        if isinstance(datum, EndOfStream):
            await self._subscriber.push(datum)
        else:
            await self._subscriber.push(Envelope(data=datum, channel=self._channel))
