import unittest
from typing import Any, AsyncGenerator, Generator

from utils.converters import iter_over_async


class TestConverters(unittest.IsolatedAsyncioTestCase):
    async def test_iter_over_async(self):
        # setup
        expected: str = 'something'

        async def a_gen() -> AsyncGenerator[Any, None]:
            yield expected

        # execute
        actual: None | str = None
        actual_gen: Generator = iter_over_async(a_iter=a_gen())
        for actual in actual_gen:
            pass

        # assess
        self.assertIsInstance(actual_gen, Generator)
        self.assertEqual(expected, actual)

        # teardown


if __name__ == '__main__':
    unittest.main()
