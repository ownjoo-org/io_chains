"""
Unit tests for Link observability: name, metrics tracking, and callbacks.

Covers:
  - name param is stored and accessible
  - items_in counts non-EOS items received via push()
  - items_out counts non-EOS items published downstream
  - items_skipped counts Skip results from processor
  - items_errored counts handled errors (on_error path)
  - elapsed_seconds is positive after a run
  - on_metrics callback is called once at EOS with a LinkMetrics instance
  - LinkMetrics fields are accurate
  - Structured log is emitted at EOS (log record contains link name and counts)
  - Enricher also participates in metrics (items_in, items_out)
  - Default name is empty string when not provided
"""

import logging
import unittest

from io_chains.links.enricher import Enricher
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector
from io_chains._internal.sentinel import Skip


class TestLinkName(unittest.TestCase):
    def test_processor_stores_name(self):
        p = Processor(name="my-processor")
        self.assertEqual(p.name, "my-processor")

    def test_default_name_is_empty_string(self):
        p = Processor()
        self.assertEqual(p.name, "")

    def test_enricher_stores_name(self):
        e = Enricher(
            name="my-enricher",
            relations=[],
            primary_channel="primary",
        )
        self.assertEqual(e.name, "my-enricher")


class TestMetricsTracking(unittest.IsolatedAsyncioTestCase):
    async def test_items_in_counts_received_items(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 3)

    async def test_items_out_counts_published_items(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_out, 3)

    async def test_items_skipped_counts_skip_results(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: Skip() if x == 2 else x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_skipped, 1)
        self.assertEqual(captured[0].items_out, 2)

    async def test_items_errored_counts_handled_errors(self):
        captured = []

        def boom(x):
            if x == 2:
                raise ValueError("bad item")
            return x

        p = Processor(
            source=[1, 2, 3],
            processor=boom,
            on_error=lambda e, datum: datum,  # recover: pass item through
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_errored, 1)
        self.assertEqual(captured[0].items_out, 3)  # all 3 still published

    async def test_elapsed_seconds_is_positive(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertGreater(captured[0].elapsed_seconds, 0)

    async def test_metrics_has_correct_name(self):
        captured = []
        p = Processor(
            source=[1],
            processor=lambda x: x,
            name="named-link",
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].name, "named-link")

    async def test_on_metrics_called_exactly_once(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(len(captured), 1)

    async def test_generator_processor_counts_multiple_outputs(self):
        """A processor that yields multiple items: items_out > items_in."""
        captured = []

        def expand(x):
            yield x
            yield x * 10

        p = Processor(
            source=[1, 2],
            processor=expand,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 2)
        self.assertEqual(captured[0].items_out, 4)

    async def test_no_processor_passthrough_still_tracked(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 3)
        self.assertEqual(captured[0].items_out, 3)

    async def test_enricher_metrics_tracked(self):
        captured = []
        results = Collector()

        enricher = Enricher(
            relations=[],
            primary_channel="primary",
            subscribers=[results],
            on_metrics=lambda m: captured.append(m),
        )

        feeder = Processor(source=[{"id": 1}, {"id": 2}])
        feeder.subscribe(enricher, channel="primary")

        from asyncio import create_task, gather

        await gather(create_task(feeder()), create_task(enricher()))

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].items_in, 2)
        self.assertEqual(captured[0].items_out, 2)

    async def test_multiple_workers_aggregate_metrics(self):
        """items_in and items_out are correct totals across all workers."""
        captured = []
        p = Processor(
            source=list(range(10)),
            processor=lambda x: x,
            workers=4,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 10)
        self.assertEqual(captured[0].items_out, 10)


class TestStructuredLogging(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._links_logger = logging.getLogger("io_chains")
        self._original_level = self._links_logger.level
        self._links_logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self._links_logger.setLevel(self._original_level)

    async def test_log_emitted_at_eos(self):
        handler = _CapturingHandler()
        self._links_logger.addHandler(handler)
        try:
            p = Processor(source=[1, 2, 3], processor=lambda x: x, name="log-test")
            await p()
        finally:
            self._links_logger.removeHandler(handler)

        self.assertTrue(len(handler.records) > 0)
        names = [r.getMessage() for r in handler.records]
        self.assertTrue(any("log-test" in msg for msg in names))

    async def test_log_record_contains_metrics_extra(self):
        handler = _CapturingHandler()
        self._links_logger.addHandler(handler)
        try:
            p = Processor(source=[1, 2], processor=lambda x: x, name="metrics-log")
            await p()
        finally:
            self._links_logger.removeHandler(handler)

        eos_records = [r for r in handler.records if hasattr(r, "items_in")]
        self.assertTrue(len(eos_records) > 0)
        r = eos_records[0]
        self.assertEqual(r.items_in, 2)
        self.assertEqual(r.items_out, 2)


class _CapturingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


if __name__ == "__main__":
    unittest.main()
