import unittest

from io_chains.pubsub.collector import Collector
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestCollector(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        # setup

        # execute
        actual: Collector = Collector()

        # assess
        self.assertIsInstance(actual, Collector)

        # teardown

    def test_should_iterate_sync(self):
        # setup
        subscriber = Collector()
        expected: str = 'something'

        # execute
        subscriber.push(expected)  # put our expected value in the queue
        subscriber.push(END_OF_STREAM)  # tell it we're done
        actual: str | None = None
        for each in subscriber:
            actual = each

        # assess
        self.assertEqual(expected, actual)

        # teardown

    async def test_should_iterate_async(self):
        # setup
        subscriber = Collector()
        expected: str = 'something'

        # execute
        subscriber.push(expected)  # put our expected value in the queue
        subscriber.push(END_OF_STREAM)  # tell it we're done
        actual: str | None = None
        async for each in subscriber:
            actual = each

        # assess
        self.assertIsInstance(subscriber, Collector)
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
