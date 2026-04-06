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
└── Collector

Publisher (ABC)
└── Linkable(Publisher, Subscriber)  (ABC)
    ├── Link
    └── Chain
```

**`Publisher`** manages a list of subscribers and distributes data to all of them via `publish(datum)`.

**`Subscriber`** defines the `push(datum)` interface for receiving data.

**`Linkable`** combines both — it can receive data (as a subscriber in one pipeline) and emit data (as a publisher to its own subscribers).

**`Link`** implements `Linkable`. Internally it uses an `asyncio.Queue` to decouple the input reader from the subscriber publisher, allowing both to run concurrently.

**`Chain`** implements `Linkable`. It wires its internal Links/Chains together, runs them all concurrently via `gather`, and presents a single `await chain()` interface to callers.

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
