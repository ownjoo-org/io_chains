import unittest

from io_chains.pubsub.collector import Collector
from io_chains.pubsub.publisher import Publisher
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestPublisher(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        actual = Publisher()
        self.assertIsInstance(actual, Publisher)

    def test_should_set_and_get_subscribers(self):
        publisher = Publisher()
        publisher.subscribers = [Collector()]
        self.assertTrue(len(publisher.subscribers))

    def test_should_reject_non_subscriber(self):
        publisher = Publisher()
        with self.assertRaises(TypeError):
            publisher.subscribers = [lambda x: x]

    async def test_should_publish_to_subscriber(self):
        publisher = Publisher()
        collector = Collector()
        publisher.subscribers = [collector]
        await publisher.publish('something')
        await publisher.publish(END_OF_STREAM)
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual('something', actual)

    async def test_should_publish_to_all_subscribers(self):
        publisher = Publisher()
        subs = [Collector(), Collector(), Collector()]
        publisher.subscribers = subs
        await publisher.publish('something')
        await publisher.publish(END_OF_STREAM)
        results = []
        for sub in subs:
            async for each in sub:
                results.append(each)
        self.assertEqual(['something', 'something', 'something'], results)


if __name__ == '__main__':
    unittest.main()
