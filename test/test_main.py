import unittest
from asyncio import run

from main import main


class TestIOChainMain(unittest.TestCase):
    def test_main(self):
        # setup

        # execute
        actual = run(main())

        # assess
        self.assertTrue(True)

        # teardown


if __name__ == '__main__':
    unittest.main()
