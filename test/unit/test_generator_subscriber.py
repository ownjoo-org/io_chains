import unittest
from collections.abc import AsyncGenerator

from io_chains.subscribables.generator_subscriber import GeneratorSubscriber


class TestGeneratorSubscriber(unittest.IsolatedAsyncioTestCase):
    async def test_should_instantiate(self):
        # setup

        # execute
        actual: GeneratorSubscriber = GeneratorSubscriber()

        # assess
        self.assertIsInstance(actual, GeneratorSubscriber)

        # teardown

    async def test_should_out(self):
        # setup
        subscriber = GeneratorSubscriber()
        expected: str = 'something'

        # execute
        actual_gen: AsyncGenerator = subscriber.out()
        await subscriber.push(expected)  # put our expected value in the queue
        await subscriber.push(None)  # tell it we're done
        actual: str | None = None
        async for each in actual_gen:
            actual = each

        # assess
        self.assertIsInstance(actual_gen, AsyncGenerator)
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
