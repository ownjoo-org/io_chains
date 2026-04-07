import unittest
from asyncio import create_task, gather
from collections.abc import AsyncGenerator
from logging import getLogger

from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector
from io_chains.pubsub.sentinel import END_OF_STREAM, SKIP

logger = getLogger()


async def gen_func() -> AsyncGenerator[str, None]:
    yield 'something'


class TestLink(unittest.IsolatedAsyncioTestCase):
    def test_link_should_instantiate(self):
        actual = Link()
        self.assertIsInstance(actual, Link)

    async def test_link_should_handle_list_source(self):
        results = Collector()
        link = Link(source=[1, 2, 3], subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 2, 3], actual)

    async def test_link_should_handle_subscriber(self):
        expected = 'something'
        collector = Collector()
        link = Link(source=[expected], subscribers=[collector])
        await link()
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual(expected, actual)

    async def test_link_input_should_generate_from_list(self):
        expected = 'something'
        link = Link(source=[expected])
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_link_input_should_generate_from_generator(self):
        expected = 'something'
        link = Link(source=(x for x in [expected]))
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_link_input_should_generate_from_async_gen_func(self):
        expected = 'something'
        link = Link(source=gen_func)
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_link_input_should_generate_from_sync_callable(self):
        # A callable that returns a plain iterable (not an async generator)
        link = Link(source=lambda: [10, 20, 30])
        actual = [item async for item in link.input]
        self.assertEqual([10, 20, 30], actual)

    async def test_link_should_publish_to_all_subscribers(self):
        expected = 'something'
        sub1 = Collector()
        sub2 = Collector()
        link = Link(source=[expected], subscribers=[sub1, sub2])
        await link()
        actual1 = None
        async for each in sub1:
            actual1 = each
        actual2 = None
        async for each in sub2:
            actual2 = each
        self.assertEqual(expected, actual1)
        self.assertEqual(expected, actual2)

    async def test_link_should_support_async_transformer(self):
        async def double(x):
            return x * 2

        results = Collector()
        link = Link(source=[1, 2, 3], transformer=double, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([2, 4, 6], actual)

    async def test_link_subscriber_mode_waits_for_upstream_eos(self):
        # A sourceless link must NOT emit END_OF_STREAM on its own —
        # it must wait for EOS to arrive via push() from upstream.
        results = Collector()
        link = Link(transformer=lambda x: x * 2, subscribers=[results])
        await link.push(5)
        await link.push(10)
        await link.push(END_OF_STREAM)
        await link()
        actual = [item async for item in results]
        self.assertEqual([10, 20], actual)

    async def test_link_should_handle_subscriber_link(self):
        expected = 'something'
        collector = Collector()
        loader = Link(transformer=lambda x: x, subscribers=[collector])
        extractor = Link(source=gen_func, subscribers=[loader])
        await gather(
            create_task(extractor()),
            create_task(loader()),
        )
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual(expected, actual)

    async def test_link_queue_size_applies_backpressure(self):
        # A bounded queue (queue_size=1) should still produce correct results —
        # the producer blocks until the consumer drains each slot.
        results = Collector()
        link = Link(source=list(range(5)), transformer=lambda x: x * 2, subscribers=[results], queue_size=1)
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 2, 4, 6, 8], actual)


    # --- Filter / drop ---

    async def test_link_transformer_returning_skip_drops_item(self):
        results = Collector()
        link = Link(
            source=[1, 2, 3, 4, 5],
            transformer=lambda x: SKIP if x % 2 == 0 else x,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 3, 5], actual)

    async def test_link_transformer_skip_all_produces_empty_stream(self):
        results = Collector()
        link = Link(source=[1, 2, 3], transformer=lambda x: SKIP, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([], actual)

    async def test_link_async_transformer_can_return_skip(self):
        results = Collector()

        async def keep_evens(x):
            return x if x % 2 == 0 else SKIP

        link = Link(source=[1, 2, 3, 4], transformer=keep_evens, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([2, 4], actual)

    # --- Flat-map / expand ---

    async def test_link_transformer_sync_generator_expands_items(self):
        results = Collector()
        # Each string is split into individual words
        link = Link(
            source=['hello world', 'foo bar baz'],
            transformer=lambda s: (word for word in s.split()),
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual(['hello', 'world', 'foo', 'bar', 'baz'], actual)

    async def test_link_transformer_async_generator_expands_items(self):
        results = Collector()

        async def expand(n):
            for i in range(n):
                yield i

        link = Link(source=[3, 2], transformer=expand, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 1, 2, 0, 1], actual)

    async def test_link_transformer_generator_returning_zero_items_filters(self):
        # A generator that yields nothing is equivalent to SKIP
        results = Collector()
        link = Link(
            source=[1, 2, 3],
            transformer=lambda x: (i for i in [] if False),  # always empty
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([], actual)

    # --- Concurrent workers ---

    async def test_link_workers_produces_correct_results(self):
        results = Collector()
        link = Link(
            source=list(range(10)),
            transformer=lambda x: x * 2,
            subscribers=[results],
            workers=3,
        )
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual(sorted([x * 2 for x in range(10)]), actual)

    async def test_link_workers_eos_propagates_exactly_once(self):
        # With N workers, downstream should still receive exactly one EOS
        # (i.e., the Collector stops after all items — not N times early or N times EOS)
        results = Collector()
        link = Link(source=list(range(5)), transformer=lambda x: x, subscribers=[results], workers=4)
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual([0, 1, 2, 3, 4], actual)

    async def test_link_workers_subscriber_only_mode(self):
        # Multi-worker subscriber-only link — upstream sends EOS once, workers share it
        results = Collector()
        link = Link(transformer=lambda x: x * 3, subscribers=[results], workers=2)
        for i in range(4):
            await link.push(i)
        await link.push(END_OF_STREAM)
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual(sorted([0, 3, 6, 9]), actual)


if __name__ == '__main__':
    unittest.main()
