from dataclasses import dataclass
from typing import Any


@dataclass
class Envelope:
    """Wraps a data item with its source channel tag."""
    data: Any
    channel: str

    def __repr__(self) -> str:
        return f'Envelope(channel={self.channel!r}, data={self.data!r})'
