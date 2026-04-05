import unittest

from io_chains.pubsub.collector import Collector
from io_chains.pubsub.publisher import Publisher
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestPublisher(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        # setup

        # execute
        actual = Publisher()

        # assess
        self.assertIsInstance(actual, Publisher)

        # teardown

    def test_should_set_get_subscribers(self):
        # setup
        publisher = Publisher()

        # execute
        publisher.subscribers = [Collector()]
        actual = publisher.subscribers

        # assess
        self.assertIsInstance(publisher, Publisher)
        self.assertTrue(len(actual))

        # teardown

    async def test_should_publish(self):
        # setup
        publisher = Publisher()
        gen_subscriber = Collector()
        publisher.subscribers = [gen_subscriber]

        # execute
        await publisher.publish('something')
        await publisher.publish(END_OF_STREAM)
        actual = None
        async for each in gen_subscriber:
            actual = each

        # assess
        self.assertIsNotNone(actual)

        # teardown

    async def test_should_publish_to_all_subscribers(self):
        # setup
        publisher = Publisher()
        gen_sub1 = Collector()
        gen_sub2 = Collector()
        gen_sub3 = Collector()
        publisher.subscribers = [gen_sub1, gen_sub2, gen_sub3]

        # execute
        await publisher.publish('something')
        await publisher.publish(END_OF_STREAM)

        results = []
        for sub in [gen_sub1, gen_sub2, gen_sub3]:
            async for each in sub:
                results.append(each)

        # assess
        self.assertEqual(results, ['something', 'something', 'something'])

        # teardown


if __name__ == '__main__':
    unittest.main()
