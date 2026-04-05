import unittest

from io_chains.pubsub.callback_subscriber import CallbackSubscriber
from io_chains.pubsub.sentinel import END_OF_STREAM


class TestCallbackSubscriber(unittest.IsolatedAsyncioTestCase):
    async def test_should_instantiate(self):
        # setup

        # execute
        actual: CallbackSubscriber = CallbackSubscriber(
            callback=lambda x: x  # passthrough
        )
        # assess
        self.assertIsInstance(actual, CallbackSubscriber)

        # teardown

    async def test_should_callback(self):
        # setup
        callback_sub: CallbackSubscriber = CallbackSubscriber(
            callback=lambda x: x  # passthrough
        )
        expected: str = 'something'

        # execute
        actual: str = callback_sub.push(expected)

        # assess
        self.assertIsInstance(callback_sub, CallbackSubscriber)
        self.assertEqual(expected, actual)

        # teardown


    def test_should_not_invoke_callback_for_end_of_stream(self):
        # END_OF_STREAM is a control signal, not data — the user's callback
        # should never be called with it
        received = []
        sub = CallbackSubscriber(callback=lambda x: received.append(x))

        sub.push('data')
        sub.push(END_OF_STREAM)

        self.assertEqual(received, ['data'])


if __name__ == '__main__':
    unittest.main()
