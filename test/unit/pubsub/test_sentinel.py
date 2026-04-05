import unittest

from io_chains.pubsub.sentinel import END_OF_STREAM, EndOfStream


class TestEndOfStream(unittest.TestCase):
    def test_should_be_instance_of_end_of_stream(self):
        self.assertIsInstance(END_OF_STREAM, EndOfStream)

    def test_should_be_singleton(self):
        another = EndOfStream()
        self.assertIs(END_OF_STREAM, another)

    def test_should_have_readable_repr(self):
        self.assertEqual(repr(END_OF_STREAM), 'END_OF_STREAM')

    def test_none_is_not_end_of_stream(self):
        self.assertNotIsInstance(None, EndOfStream)
        self.assertIsNot(None, END_OF_STREAM)


if __name__ == '__main__':
    unittest.main()
