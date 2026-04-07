class EndOfStream:
    """Sentinel signalling end of data in a stream. Singleton."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "END_OF_STREAM"


class Skip:
    """Sentinel returned by a transformer to drop the current item from the stream."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "SKIP"


END_OF_STREAM = EndOfStream()
SKIP = Skip()
