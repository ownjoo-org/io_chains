# converters.py — retained for backwards-compatibility; iter_over_async removed in 0.1.0.
# Collector now uses asyncio.Queue.get_nowait() for sync iteration, eliminating nest_asyncio.
