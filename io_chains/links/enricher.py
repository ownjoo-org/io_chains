from collections.abc import Callable
from dataclasses import dataclass

from io_chains._internal.link import Link
from io_chains._internal.envelope import Envelope
from io_chains._internal.sentinel import END_OF_STREAM, EndOfStream, Skip


@dataclass
class Relation:
    """Describes a foreign-key-style join between the primary channel and a side channel.

    Attributes:
        from_field:  field on the primary item (its value is the join key).
                     For many=True, the value should be a list of keys.
        to_channel:  name of the channel that holds the related items.
        to_field:    field on the related items to match against from_field.
        attach_as:   key to add to the enriched primary item.
        many:        True → one-to-many (attach list); False → one-to-one (attach single or None).
        key_transform: optional callable applied to each key before lookup.
    """

    from_field: str
    to_channel: str
    to_field: str
    attach_as: str
    many: bool = False
    key_transform: Callable | None = None


class Enricher(Link):
    """
    Collects all side-channel items first, then streams primary items enriched
    via the declared Relations.

    Usage:
        enricher = Enricher(relations=[...], primary_channel='chars', subscribers=[results])
        chars_link.subscribe(enricher, channel='chars')
        loc_link.subscribe(enricher, channel='locations')
        ep_link.subscribe(enricher, channel='episodes')

        await gather(
            create_task(chars_link()),
            create_task(loc_link()),
            create_task(ep_link()),
            create_task(enricher()),
        )

    The Enricher counts distinct EOS signals (one per subscribed upstream).
    Primary items are buffered until all channels have finished; then they are
    enriched and published in arrival order.
    """

    def __init__(
        self,
        *args,
        relations: list[Relation],
        primary_channel: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._relations = relations
        self._primary_channel = primary_channel

    # ------------------------------------------------------------------ input (unused)

    @property
    async def input(self):
        return
        yield  # make it an async generator

    # ------------------------------------------------------------------ run

    async def run(self) -> None:
        self._reset_metrics()

        # Partition incoming Envelopes into per-channel lists
        side_channels: dict[str, list] = {}
        primary_items: list = []

        while True:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                break
            if isinstance(datum, Envelope):
                if datum.channel == self._primary_channel:
                    primary_items.append(datum.data)
                else:
                    side_channels.setdefault(datum.channel, []).append(datum.data)

        # Build lookup dicts for each relation's channel
        lookups: dict[str, dict] = {}
        for rel in self._relations:
            channel_items = side_channels.get(rel.to_channel, [])
            lookups[rel.to_channel] = {item[rel.to_field]: item for item in channel_items}

        # Enrich and publish primary items
        for item in primary_items:
            try:
                enriched = dict(item)
                for rel in self._relations:
                    lookup = lookups.get(rel.to_channel, {})
                    key_fn = rel.key_transform or (lambda x: x)
                    if rel.many:
                        raw_keys = item.get(rel.from_field, [])
                        enriched[rel.attach_as] = [lookup[key_fn(k)] for k in raw_keys if key_fn(k) in lookup]
                    else:
                        raw_key = item.get(rel.from_field)
                        enriched[rel.attach_as] = lookup.get(key_fn(raw_key)) if raw_key is not None else None
            except Exception as e:
                self._items_errored += 1
                if self._on_error is not None:
                    result = self._on_error(e, item)
                    if hasattr(result, "__await__"):
                        result = await result
                    if result is None or isinstance(result, Skip):
                        continue
                    enriched = result
                else:
                    raise
            await self.publish(enriched)

        await self.publish(END_OF_STREAM)
        await self._emit_metrics()
