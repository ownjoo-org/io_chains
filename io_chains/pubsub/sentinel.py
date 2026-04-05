class EndOfStream:
    """Sentinel signaling end of data in a stream. Implemented as a singleton."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return 'END_OF_STREAM'


END_OF_STREAM = EndOfStream()
