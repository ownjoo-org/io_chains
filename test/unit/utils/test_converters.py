import unittest

# iter_over_async was removed in 0.1.0.
# Collector now uses asyncio.Queue.get_nowait() for sync iteration, eliminating nest_asyncio.
# This file is retained as a placeholder.


class TestConverters(unittest.TestCase):
    def test_placeholder(self):
        pass


if __name__ == '__main__':
    unittest.main()
