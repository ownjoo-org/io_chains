from dataclasses import dataclass


@dataclass
class LinkMetrics:
    name: str
    items_in: int
    items_out: int
    items_skipped: int
    items_errored: int
    elapsed_seconds: float
