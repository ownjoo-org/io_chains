import unittest

from io_chains.pubsub.callback_subscriber import CallbackSubscriber
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestCallbackSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_should_instantiate(self):
        actual = CallbackSubscriber(callback=lambda x: x)
        self.assertIsInstance(actual, CallbackSubscriber)

    async def test_should_callback(self):
        sub = CallbackSubscriber(callback=lambda x: x)  # passthrough
        expected = 'something'
        actual = await sub.push(expected)
        self.assertEqual(expected, actual)

    async def test_should_support_async_callback(self):
        async def async_double(x):
            return x * 2

        sub = CallbackSubscriber(callback=async_double)
        actual = await sub.push(3)
        self.assertEqual(6, actual)

    async def test_should_not_invoke_callback_for_end_of_stream(self):
        received = []
        sub = CallbackSubscriber(callback=lambda x: received.append(x))
        await sub.push('data')
        await sub.push(END_OF_STREAM)
        self.assertEqual(['data'], received)


if __name__ == '__main__':
    unittest.main()
