# io-chains

A lightweight Python library for building async data pipelines using a publisher/subscriber pattern. Chain together data sources, transformations, and consumers with minimal boilerplate.

## Installation

```bash
pip install oj-io-chains
```

## Core Concepts

### The Pipeline Model

A pipeline is built from **Links** coordinated by a **Chain**:

```
[source] → Link → [transformer] → Subscriber(s)
                                    ├── CallbackSubscriber  (side effects)
                                    ├── Collector           (collect results)
                                    └── Link                (next stage)
```

Each `Link`:
- Pulls items from a source (list, generator, async generator, or callable)
- Optionally transforms each item (sync or async)
- Publishes each item to one or more subscribers

A `Chain` wires multiple Links together and runs them concurrently — the caller just `await chain()`.

For fan-in enrichment (joining data from multiple concurrent sources), use **`Enricher`** with **`Relation`** declarations and channel-tagged subscriptions:

```
fetch_characters ─ subscribe(enricher, channel='chars')    ─┐
fetch_episodes   ─ subscribe(enricher, channel='episodes') ─┤ Enricher → subscribers
fetch_locations  ─ subscribe(enricher, channel='locations')─┘
```

### End-of-stream

`EndOfStream` is the sentinel that signals end-of-stream through the pipeline. `None` is valid data and flows through the pipeline normally.

---

## Classes

### `Link`

A single processing unit: source → transformer → subscribers.

```python
from io_chains.links.link import Link

link = Link(
    source=...,        # input source (optional)
    transformer=...,   # transform function (optional)
    subscribers=[...], # subscribers (optional)
)
await link()
```

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `source` | `Iterable`, callable, or `AsyncIterable` | Input data source. If a callable, it is called and the result iterated. If omitted, the Link acts as a subscriber-only stage that receives data via `push()`. |
| `transformer` | callable | Applied to each item before publishing. Can be sync or async. |
| `subscribers` | `Subscriber`, callable, or list of either | Where output is sent. |

**Source types**

```python
# List or any iterable
Link(source=[1, 2, 3])

# Generator expression
Link(source=(x * 2 for x in range(10)))

# Async generator function (called automatically)
async def my_source():
    yield 1
    yield 2

Link(source=my_source)
```

---

### `Chain`

Orchestrates multiple Links as a single pipeline. Wires them together and manages concurrent execution internally.

```python
from io_chains.links.chain import Chain

pipeline = Chain(
    source=...,        # attached to the first link (optional)
    links=[...],       # ordered list of Links or Chains
    subscribers=[...], # attached to the last link's output (optional)
)
await pipeline()       # no gather() needed
```

A Chain is itself a Linkable — it can be nested inside another Chain or used as a subscriber of an external Link.

---

### `Collector`

Buffers items for iteration after the pipeline completes. Supports both async and sync iteration directly.

```python
from io_chains.pubsub.collector import Collector

results = Collector()

# async iteration (preferred)
async for item in results:
    print(item)

# sync iteration
for item in results:
    print(item)
```

---

### `CallbackSubscriber`

Calls a function for each item. Good for side effects (logging, writing, printing).

```python
from io_chains.pubsub.callback_subscriber import CallbackSubscriber

sub = CallbackSubscriber(callback=lambda x: print(x))
```

---

### `Enricher` and `Relation`

`Enricher` is a fan-in `Linkable` that collects data from multiple named channels, then streams primary items enriched via declared `Relation` joins.

Use `subscribe(subscriber, channel=...)` to tag each upstream Link's output with a channel name. `Enricher` buffers all side-channel data until every upstream has finished, then enriches each primary item and publishes it.

```python
from io_chains.links.enricher import Enricher, Relation
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

results = Collector()

relations = [
    Relation(
        from_field='location_id',   # field on the primary item (FK)
        to_channel='locations',     # channel that holds related items
        to_field='id',              # field on related items to match against
        attach_as='location_detail', # key added to the enriched item
    ),
    Relation(
        from_field='episode_ids',
        to_channel='episodes',
        to_field='id',
        attach_as='episode_details',
        many=True,                  # one-to-many: attaches a list
    ),
]

enricher = Enricher(
    relations=relations,
    primary_channel='chars',
    subscribers=[results],
)

chars_link     = Link(source=chars_source)
episodes_link  = Link(source=episodes_source)
locations_link = Link(source=locations_source)

chars_link.subscribe(enricher, channel='chars')
episodes_link.subscribe(enricher, channel='episodes')
locations_link.subscribe(enricher, channel='locations')

await gather(
    create_task(chars_link()),
    create_task(episodes_link()),
    create_task(locations_link()),
    create_task(enricher()),
)
```

**`Relation` parameters**

| Parameter | Type | Description |
|---|---|---|
| `from_field` | `str` | Field on the primary item whose value is the join key. For `many=True`, the value should be a list of keys. |
| `to_channel` | `str` | Channel name that holds the related items. |
| `to_field` | `str` | Field on related items to match against `from_field`. |
| `attach_as` | `str` | Key added to the enriched primary item. |
| `many` | `bool` | `True` → one-to-many (attach list); `False` → one-to-one (attach single or `None`). |
| `key_transform` | callable or `None` | Optional transform applied to each key before lookup. |

---

### `Limit`

A stateful callable transformer that passes through the first `n` items and returns `SKIP` for all subsequent ones. Use it as a `transformer=` on a `Link`.

```python
from io_chains.links.limit import Limit
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

results = Collector()
link = Link(source=range(100), transformer=Limit(5), subscribers=[results])
await link()
# results contains [0, 1, 2, 3, 4]
```

Because `Limit` is stateful, create a new instance per pipeline run.

---

### `subscribe(subscriber, channel=None)`

`Publisher.subscribe` (available on `Link`, `Chain`, and `Enricher`) wires a subscriber, optionally tagging each item with a channel label.

```python
# plain subscription — equivalent to passing sub in subscribers=[...]
link.subscribe(sub)

# channel-tagged subscription — each item arrives wrapped in Envelope(data, channel)
link.subscribe(enricher, channel='chars')
```

When a channel is given, items are wrapped in an `Envelope(data, channel)` before being pushed to the subscriber. `EndOfStream` is always forwarded unwrapped so pipeline termination propagates correctly.

---

## Usage Examples

### Simple transformation

```python
import asyncio
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

async def main():
    results = Collector()
    link = Link(
        source=[1, 2, 3],
        transformer=lambda x: x * 2,
        subscribers=[results],
    )
    await link()

    async for item in results:
        print(item)  # 2, 4, 6

asyncio.run(main())
```

### Multi-stage pipeline with Chain

`Chain` manages concurrent execution — no `gather()` or `create_task()` needed.

```python
import asyncio
from io_chains.links.chain import Chain
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

async def main():
    results = Collector()

    pipeline = Chain(
        source=['a', 'b', 'c'],
        links=[
            Link(transformer=str.upper),
            Link(transformer=lambda x: f'item: {x}'),
        ],
        subscribers=[results],
    )
    await pipeline()

    async for item in results:
        print(item)
    # item: A
    # item: B
    # item: C

asyncio.run(main())
```

### Async generator source with enrichment

```python
from httpx import AsyncClient
from io_chains.links.chain import Chain
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

async def fetch_records():
    async with AsyncClient() as client:
        response = await client.get('https://api.example.com/records')
        for record in response.json():
            yield record

async def main():
    results = Collector()

    pipeline = Chain(
        source=fetch_records,
        links=[
            Link(transformer=lambda r: {**r, 'name': r['name'].upper()}),
        ],
        subscribers=[results],
    )
    await pipeline()

    async for record in results:
        print(record)
```

### Concurrent fan-in with enrichment

Fetch characters, episodes, and locations concurrently, then enrich each character with its related location and episodes.

```python
import asyncio
from asyncio import create_task, gather
from httpx import AsyncClient
from io_chains.links.enricher import Enricher, Relation
from io_chains.links.limit import Limit
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

BASE_URL = 'https://rickandmortyapi.com/api'

async def fetch_characters(client):
    resp = await client.get(f'{BASE_URL}/character')
    for c in resp.json()['results']:
        # pre-compute integer FK fields the Enricher needs
        c['location_id'] = int(c['location']['url'].split('/')[-1] or 0) or None
        c['episode_ids'] = [int(u.split('/')[-1]) for u in c['episode']]
        yield c

async def fetch_episodes(client):
    resp = await client.get(f'{BASE_URL}/episode')
    for ep in resp.json()['results']:
        yield ep

async def fetch_locations(client):
    resp = await client.get(f'{BASE_URL}/location')
    for loc in resp.json()['results']:
        yield loc

async def main():
    async with AsyncClient() as client:
        results = Collector()

        enricher = Enricher(
            relations=[
                Relation(from_field='location_id', to_channel='locations',
                         to_field='id', attach_as='location_detail'),
                Relation(from_field='episode_ids', to_channel='episodes',
                         to_field='id', attach_as='episode_details', many=True),
            ],
            primary_channel='chars',
            subscribers=[Link(transformer=Limit(2), subscribers=[results])],
        )
        limit_link = enricher.subscribers[0]

        chars_link     = Link(source=fetch_characters(client))
        episodes_link  = Link(source=fetch_episodes(client))
        locations_link = Link(source=fetch_locations(client))

        chars_link.subscribe(enricher, channel='chars')
        episodes_link.subscribe(enricher, channel='episodes')
        locations_link.subscribe(enricher, channel='locations')

        await gather(
            create_task(chars_link()),
            create_task(episodes_link()),
            create_task(locations_link()),
            create_task(enricher()),
            create_task(limit_link()),
        )

    async for char in results:
        print(char['name'], char['location_detail']['name'])
        # Rick Sanchez   Citadel of Ricks
        # Morty Smith    Earth (C-137)

asyncio.run(main())
```

---

### Multiple subscribers (fan-out)

Every subscriber receives every item independently.

```python
from io_chains.pubsub.callback_subscriber import CallbackSubscriber

audit = []
results = Collector()

pipeline = Chain(
    source=[1, 2, 3],
    links=[Link(transformer=lambda x: x * 2)],
    subscribers=[
        results,
        CallbackSubscriber(callback=lambda x: audit.append(x)),
    ],
)
await pipeline()
# results contains [2, 4, 6], audit == [2, 4, 6]
```

### Nested Chains

Chains can be nested — each is a black-box Linkable from the outside.

```python
normalise = Chain(links=[
    Link(transformer=lambda x: abs(x)),
    Link(transformer=lambda x: round(x, 2)),
])

stringify = Chain(links=[
    Link(transformer=lambda x: x * 100),
    Link(transformer=lambda x: f'{x:.0f}%'),
])

pipeline = Chain(
    source=[-0.156, 0.999, -0.301],
    links=[normalise, stringify],
    subscribers=[results],
)
await pipeline()
# results contains ['16%', '100%', '30%']
```

---

## Architecture

```
Subscriber (ABC)
├── CallbackSubscriber
├── Collector
└── ChannelSubscriber   (wraps another Subscriber; tags each item with a channel name)

Publisher (ABC)
└── Linkable(Publisher, Subscriber)  (ABC)
    ├── Link
    ├── Chain
    └── Enricher        (fan-in: collects side channels, enriches primaries)
```

**`Publisher`** manages a list of subscribers and distributes data to all of them via `publish(datum)`. Its `subscribe(sub, channel=None)` method optionally wraps the subscriber in a `ChannelSubscriber` so items arrive tagged with a channel name.

**`Subscriber`** defines the `push(datum)` interface for receiving data.

**`Linkable`** combines both — it can receive data (as a subscriber in one pipeline) and emit data (as a publisher to its own subscribers).

**`Link`** implements `Linkable`. Internally it uses an `asyncio.Queue` to decouple the input reader from the subscriber publisher, allowing both to run concurrently.

**`Chain`** implements `Linkable`. It wires its internal Links/Chains together, runs them all concurrently via `gather`, and presents a single `await chain()` interface to callers.

**`Enricher`** implements `Linkable`. It receives `Envelope`-wrapped items from multiple named channels via `push()`, buffers all side-channel data until every upstream finishes, then enriches each primary item according to its `Relation` list and publishes the results.

**`ChannelSubscriber`** is an internal adapter created automatically by `subscribe(sub, channel=...)`. It wraps each data item in an `Envelope(data, channel)` before forwarding to the real subscriber, while passing `EndOfStream` through unwrapped.

**`Limit(n)`** is a stateful callable transformer. It passes through the first `n` items and returns `SKIP` for all subsequent ones. Use it as `transformer=Limit(n)` on a `Link`.

---

## Development

```bash
# Install in editable mode
pip install -e .

# Run unit tests
python -m pytest test/unit -v

# Run user acceptance tests (requires network)
python -m pytest test/ua -v
```
