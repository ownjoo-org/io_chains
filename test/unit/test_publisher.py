import unittest

from io_chains.subscribables.generator_subscriber import GeneratorSubscriber
from io_chains.subscribables.publisher import Publisher


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
        publisher.subscribers = [GeneratorSubscriber()]
        actual = publisher.subscribers

        # assess
        self.assertIsInstance(publisher, Publisher)
        self.assertTrue(len(actual))

        # teardown

    async def test_should_publish(self):
        # setup
        publisher = Publisher()
        gen_subscriber = GeneratorSubscriber()
        publisher.subscribers = [gen_subscriber]

        # execute
        await publisher.publish('something')
        await publisher.publish(None)
        actual = None
        async for each in gen_subscriber.out():
            actual = each

        # assess
        self.assertIsNotNone(actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
