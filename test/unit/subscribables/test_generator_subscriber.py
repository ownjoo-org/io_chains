import unittest
from typing import Generator, AsyncGenerator

from io_chains.subscribables.generator_subscriber import GeneratorSubscriber


class TestGeneratorSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        # setup

        # execute
        actual: GeneratorSubscriber = GeneratorSubscriber()

        # assess
        self.assertIsInstance(actual, GeneratorSubscriber)

        # teardown

    def test_should_out(self):
        # setup
        subscriber = GeneratorSubscriber()
        expected: str = 'something'

        # execute
        actual_gen: Generator = subscriber.out()
        subscriber.push(expected)  # put our expected value in the queue
        subscriber.push(None)  # tell it we're done
        actual: str | None = None
        for each in actual_gen:
            actual = each

        # assess
        self.assertIsInstance(actual_gen, Generator)
        self.assertEqual(expected, actual)

        # teardown

    async def test_should_a_out(self):
        # setup
        subscriber = GeneratorSubscriber()
        expected: str = 'something'

        # execute
        actual_gen: AsyncGenerator = subscriber.a_out()
        subscriber.push(expected)  # put our expected value in the queue
        subscriber.push(None)  # tell it we're done
        actual: str | None = None
        async for each in actual_gen:
            actual = each

        # assess
        self.assertIsInstance(actual_gen, AsyncGenerator)
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
