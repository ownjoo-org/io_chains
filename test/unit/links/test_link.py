import unittest
from asyncio import create_task, gather
from collections.abc import AsyncGenerator
from logging import getLogger

from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

logger = getLogger()


async def gen_func() -> AsyncGenerator[str, None]:
    try:
        yield 'something'
    except Exception as e:
        logger.exception(f'gen_func: {e}')


class TestLink(unittest.IsolatedAsyncioTestCase):
    def test_link_should_instantiate(self):
        # setup

        # execute
        actual = Link()

        # assess
        self.assertIsInstance(actual, Link)

        # teardown

    async def test_link_should_handle_subscriber_lambda(self):
        # setup

        # execute
        actual = Link(
            source=[0, 1, 2],
            subscribers=[
                lambda value: print(f'LIST VAL: {value}'),
            ],
        )
        await actual()

        # assess
        self.assertIsInstance(actual, Link)

        # teardown

    async def test_link_should_handle_subscriber(self):
        # setup
        expected: str = 'something'
        gen_sub: Collector = Collector()
        link: Link = Link(
            source=[expected],
            subscribers=[
                gen_sub,
            ],
        )

        # execute
        await link()
        actual: str | None = None
        async for each in gen_sub:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown

    async def test_link_input_should_generate_from_list(self):
        # setup
        expected: str = 'something'

        link: Link = Link(
            source=[expected],
        )

        # execute
        actual = None
        async for each in link.input:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown

    async def test_link_input_should_generate_from_generator(self):
        # setup
        expected: str = 'something'
        expected_list: list = [expected]

        link: Link = Link(
            source=(x for x in expected_list),
        )

        # execute
        actual = None
        async for each in link.input:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown

    async def test_link_input_should_generate_from_func(self):
        # setup
        expected: str = 'something'

        link: Link = Link(
            source=gen_func,
        )

        # execute
        actual = None
        async for each in link.input:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown

    async def test_link_should_publish_to_all_subscribers(self):
        # setup
        expected: str = 'something'
        gen_sub1: Collector = Collector()
        gen_sub2: Collector = Collector()
        link: Link = Link(
            source=[expected],
            subscribers=[gen_sub1, gen_sub2],
        )

        # execute
        await link()
        actual1: str | None = None
        async for each in gen_sub1:
            actual1 = each
        actual2: str | None = None
        async for each in gen_sub2:
            actual2 = each

        # assess
        self.assertEqual(expected, actual1)
        self.assertEqual(expected, actual2)

        # teardown

    async def test_link_should_support_async_transformer(self):
        # An async transformer should be awaited and produce the correct result
        gen_sub = Collector()

        async def double(x):
            return x * 2

        link = Link(source=[1, 2, 3], transformer=double, subscribers=[gen_sub])
        await link()

        actual = [item async for item in gen_sub]
        self.assertEqual(actual, [2, 4, 6])

    async def test_link_subscriber_mode_waits_for_upstream_eos(self):
        # A link with no in_iter must NOT push a spurious END_OF_STREAM on startup.
        # It should terminate only when END_OF_STREAM arrives via push() from upstream.
        from io_chains.pubsub.sentinel import END_OF_STREAM
        gen_sub = Collector()
        link = Link(transformer=lambda x: x * 2, subscribers=[gen_sub])

        # Simulate upstream: push data then signal end
        link.push(5)
        link.push(10)
        link.push(END_OF_STREAM)

        await link()

        actual = [item async for item in gen_sub]
        self.assertEqual(actual, [10, 20])

    async def test_link_should_handle_subscriber_link(self):
        # setup
        expected: str = 'something'
        gen_sub: Collector = Collector()

        loader_link: Link = Link(
            transformer=lambda x: x,
            subscribers=[
                gen_sub,
            ],
        )

        extract_link: Link = Link(
            source=gen_func,
            subscribers=[
                loader_link,
            ],
        )

        # execute
        await gather(
            create_task(extract_link()),
            create_task(loader_link()),
        )
        actual: str | None = None
        async for each in gen_sub:
            actual = each

        # assess
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
