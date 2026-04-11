import unittest

from io_chains._internal.sentinel import END_OF_STREAM, SKIP, EndOfStream, Skip


class TestEndOfStream(unittest.TestCase):
    def test_should_be_instance_of_end_of_stream(self):
        self.assertIsInstance(END_OF_STREAM, EndOfStream)

    def test_should_be_singleton(self):
        another = EndOfStream()
        self.assertIs(END_OF_STREAM, another)

    def test_should_have_readable_repr(self):
        self.assertEqual(repr(END_OF_STREAM), "END_OF_STREAM")

    def test_none_is_not_end_of_stream(self):
        self.assertNotIsInstance(None, EndOfStream)
        self.assertIsNot(None, END_OF_STREAM)


class TestSkip(unittest.TestCase):
    def test_should_be_instance_of_skip(self):
        self.assertIsInstance(SKIP, Skip)

    def test_should_be_singleton(self):
        another = Skip()
        self.assertIs(SKIP, another)

    def test_should_have_readable_repr(self):
        self.assertEqual(repr(SKIP), "SKIP")

    def test_is_distinct_from_end_of_stream(self):
        self.assertIsNot(SKIP, END_OF_STREAM)
        self.assertNotIsInstance(SKIP, EndOfStream)


if __name__ == "__main__":
    unittest.main()
