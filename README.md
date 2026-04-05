# io_chains

A lightweight Python library for building async data pipelines using a publisher/subscriber pattern. Chain together data sources, transformations, and consumers with minimal boilerplate.

## Installation

```bash
pip install io_chains
```

## Core Concepts

### The Pipeline Model

A pipeline is built from **Links** connected by **Subscribers**:

```
[input source] → Link → [transformer] → Subscriber(s)
                                          ├── CallbackSubscriber  (side effects)
                                          ├── GeneratorSubscriber (collect results)
                                          └── Link                (chain to next stage)
```

Each `Link`:
- Pulls items from an input source (list, generator, async generator, or callable)
- Optionally transforms each item
- Publishes each item to one or more subscribers concurrently

### End-of-stream

`None` is the sentinel value that signals end-of-stream through the pipeline. Do not publish `None` as a data value.

---

## Classes

### `Link`

The central class. Combines a data source, an optional transformer, and one or more subscribers.

```python
from io_chains.linkables.link import Link

link = Link(
    in_iter=...,       # input source (optional)
    transformer=...,   # transform function (optional)
    subscribers=[...], # subscribers (optional)
)
await link()
```

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `in_iter` | `Iterable`, `callable`, or `AsyncIterable` | Input data source. If a callable, it is called and the result iterated. If omitted, the Link acts as a subscriber-only stage that receives data via `push()`. |
| `transformer` | `callable` | Applied to each item before publishing. Can be sync or async. |
| `subscribers` | `Subscriber`, `callable`, or list of either | Where output is sent. |

**Input source types**

```python
# List or any iterable
Link(in_iter=[1, 2, 3])

# Generator expression
Link(in_iter=(x * 2 for x in range(10)))

# Async generator function (called automatically)
async def my_source():
    yield 1
    yield 2

Link(in_iter=my_source)

# Async generator instance
Link(in_iter=my_source())
```

---

### `CallbackSubscriber`

Calls a function for each item. Good for side effects (logging, writing, printing).

```python
from io_chains.subscribables.callback_subscriber import CallbackSubscriber

sub = CallbackSubscriber(callback=lambda x: print(x))
```

---

### `GeneratorSubscriber`

Buffers items in a queue. Supports both async and sync iteration to collect results.

```python
from io_chains.subscribables.generator_subscriber import GeneratorSubscriber

sub = GeneratorSubscriber()

# async iteration
async for item in sub.a_out():
    print(item)

# sync iteration
for item in sub.out():
    print(item)
```

---

## Usage Examples

### Simple transformation

```python
import asyncio
from io_chains.linkables.link import Link
from io_chains.subscribables.generator_subscriber import GeneratorSubscriber

async def main():
    results = GeneratorSubscriber()
    link = Link(
        in_iter=[1, 2, 3],
        transformer=lambda x: x * 2,
        subscribers=[results],
    )
    await link()

    async for item in results.a_out():
        print(item)  # 2, 4, 6

asyncio.run(main())
```

### Multiple subscribers

Every subscriber receives every item independently.

```python
async def main():
    log = CallbackSubscriber(callback=lambda x: print(f'log: {x}'))
    results = GeneratorSubscriber()

    link = Link(
        in_iter=['a', 'b', 'c'],
        subscribers=[log, results],
    )
    await link()

    async for item in results.a_out():
        print(f'collected: {item}')
```

### Chained links (multi-stage pipeline)

When a `Link` is used as a subscriber, it receives items via `push()` into its internal queue. The downstream link must be run concurrently with `gather`.

```python
import asyncio
from asyncio import gather, create_task
from io_chains.linkables.link import Link
from io_chains.subscribables.generator_subscriber import GeneratorSubscriber

async def main():
    results = GeneratorSubscriber()

    stage2 = Link(
        transformer=lambda x: f'processed: {x}',
        subscribers=[results],
    )

    stage1 = Link(
        in_iter=['a', 'b', 'c'],
        subscribers=[stage2],
    )

    await gather(
        create_task(stage1()),
        create_task(stage2()),
    )

    async for item in results.a_out():
        print(item)
    # processed: a
    # processed: b
    # processed: c

asyncio.run(main())
```

### Async generator source

```python
from httpx import AsyncClient

async def fetch_pages():
    async with AsyncClient() as client:
        for page in range(1, 4):
            response = await client.get(f'https://api.example.com/items?page={page}')
            yield response.json()

async def main():
    results = GeneratorSubscriber()
    link = Link(
        in_iter=fetch_pages,   # pass the function, not the instance
        transformer=lambda data: data['items'],
        subscribers=[results],
    )
    await link()

    async for items in results.a_out():
        print(items)
```

---

## Architecture

```
Subscriber (ABC)
└── CallbackSubscriber
└── GeneratorSubscriber

Publisher (ABC)
└── Linkable(Publisher, Subscriber)  (ABC)
    └── Link
```

**`Publisher`** manages a list of subscribers and distributes data to all of them via `publish(datum)`.

**`Subscriber`** defines the `push(datum)` interface for receiving data.

**`Linkable`** combines both — it can receive data (as a subscriber in one pipeline) and emit data (as a publisher to its own subscribers).

**`Link`** implements `Linkable`. Internally it uses an `asyncio.Queue` to decouple the input reader (`_fill_queue_from_input`) from the subscriber publisher (`_update_subscribers`), allowing both to run concurrently.

---

## Development

```bash
# Install in editable mode
pip install -e .

# Run unit tests
python -m pytest test/unit -v

# Run integration tests (requires network)
python -m pytest test/int -v
```
