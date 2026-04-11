"""
Unit tests for graceful shutdown / cancellation propagation.

When a running pipeline task is cancelled:
  - CancelledError propagates out of run() (not swallowed)
  - Downstream links receive EOS so they don't hang waiting forever
  - Collector downstream of a cancelled Link becomes iterable (not blocked)
  - Enricher downstream of a cancelled source receives EOS and terminates

These tests use asyncio.wait_for with a timeout to simulate external cancellation
and verify that the downstream does not hang indefinitely.
"""

import asyncio
import unittest
from asyncio import TaskGroup

from io_chains.links.enricher import Enricher
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector


async def _infinite_source():
    """An async generator that never ends — used to test cancellation."""
    i = 0
    while True:
        yield i
        i += 1
        await asyncio.sleep(0)  # yield control so cancellation can land


class TestProcessorGracefulShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_processor_propagates_eos_downstream(self):
        """Cancelling a running Link causes EOS to reach downstream."""
        results = Collector()

        source = Processor(source=_infinite_source, processor=lambda x: x)
        sink = Processor(processor=lambda x: x, subscribers=[results])
        source.subscribers = [sink]

        async def run():
            async with TaskGroup() as tg:
                tg.create_task(source())
                tg.create_task(sink())

        task = asyncio.create_task(run())
        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises((asyncio.CancelledError, ExceptionGroup)):
            await task

        async with asyncio.timeout(1.0):
            drained = [item async for item in results]

        self.assertIsInstance(drained, list)

    async def test_cancelled_processor_sends_eos_not_stuck(self):
        """After cancellation the downstream Collector terminates without hanging."""
        results = Collector()

        p = Processor(source=_infinite_source, processor=lambda x: x, subscribers=[results])

        task = asyncio.create_task(p())
        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

        async with asyncio.timeout(1.0):
            items = [item async for item in results]

        self.assertIsInstance(items, list)

    async def test_cancellation_error_reraises(self):
        """CancelledError is not swallowed — it propagates to the caller."""
        p = Processor(source=_infinite_source, processor=lambda x: x)

        task = asyncio.create_task(p())
        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task


class TestEnricherGracefulShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_propagates_eos_to_enricher(self):
        """Cancelling concurrent links — Enricher should receive EOS and not hang."""
        results = Collector()

        enricher = Enricher(
            relations=[],
            primary_channel="chars",
            subscribers=[results],
        )

        chars = Processor(source=_infinite_source)
        chars.subscribe(enricher, channel="chars")

        async def run():
            async with TaskGroup() as tg:
                tg.create_task(chars())
                tg.create_task(enricher())

        task = asyncio.create_task(run())
        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises((asyncio.CancelledError, ExceptionGroup)):
            await task

        async with asyncio.timeout(1.0):
            items = [item async for item in results]

        self.assertIsInstance(items, list)


if __name__ == "__main__":
    unittest.main()
