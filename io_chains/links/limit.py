from io_chains.pubsub.sentinel import SKIP


class Limit:
    """Stateful callable: passes through the first n items, returns SKIP for the rest."""

    def __init__(self, n: int) -> None:
        self._remaining = n

    def __call__(self, item):
        if self._remaining > 0:
            self._remaining -= 1
            return item
        return SKIP
