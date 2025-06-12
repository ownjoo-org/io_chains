import unittest

from io_chains.subscribables.callback_subscriber import CallbackSubscriber


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


if __name__ == '__main__':
    unittest.main()
