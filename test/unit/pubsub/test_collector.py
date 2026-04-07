import unittest

from io_chains.pubsub.collector import Collector
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestCollector(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        actual = Collector()
        self.assertIsInstance(actual, Collector)

    async def test_should_iterate_async(self):
        collector = Collector()
        expected = "something"
        await collector.push(expected)
        await collector.push(END_OF_STREAM)
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual(expected, actual)

    async def test_should_iterate_sync_after_pipeline(self):
        # Sync iteration drains buffered items — valid after the pipeline has completed.
        collector = Collector()
        await collector.push("a")
        await collector.push("b")
        await collector.push(END_OF_STREAM)
        actual = list(collector)
        self.assertEqual(["a", "b"], actual)

    async def test_should_respect_maxsize_with_backpressure(self):
        # maxsize includes the EOS sentinel, so reserve one slot for it.
        collector = Collector(maxsize=3)
        await collector.push(1)
        await collector.push(2)
        await collector.push(END_OF_STREAM)
        actual = list(collector)
        self.assertEqual([1, 2], actual)


if __name__ == "__main__":
    unittest.main()
