import unittest
from collections.abc import AsyncGenerator
from logging import getLogger

from io_chains.linkables.link import Link
from io_chains.subscribables.generator_subscriber import GeneratorSubscriber

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
            in_iter=[0, 1, 2],
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
        gen_sub: GeneratorSubscriber = GeneratorSubscriber()
        link: Link = Link(
            in_iter=[expected],
            subscribers=[
                gen_sub,
            ],
        )

        # execute
        await link()
        actual_gen = gen_sub.out()
        actual: str | None = None
        async for each in actual_gen:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown

    async def test_link_input_should_generate_from_list(self):
        # setup
        expected: str = 'something'

        link: Link = Link(
            in_iter=[expected],
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
            in_iter=(x for x in expected_list),
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
            in_iter=gen_func,
        )

        # execute
        actual = None
        async for each in link.input:
            actual = each

        # assess
        self.assertIsInstance(link, Link)
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
