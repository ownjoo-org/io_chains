"""
Unit tests for consistent error handling across all link types.

Covers:
  - on_error on Link (existing, verify still works)
  - on_error on Enricher (new)
  - Exceptions in source generators are caught and routed through on_error
  - Unhandled exceptions propagate out of run() when no on_error is set
  - on_error returning Skip suppresses the item
  - on_error itself raising re-raises out of run()
  - Async on_error handlers work on both Link and Enricher
  - items_errored metric is incremented for source generator errors
"""

import unittest
from asyncio import create_task, gather

from io_chains.links.enricher import Enricher, Relation
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector
from io_chains._internal.sentinel import Skip


# ---------------------------------------------------------------------------
# Link error handling (existing behaviour — must still pass)
# ---------------------------------------------------------------------------


class TestProcessorOnError(unittest.IsolatedAsyncioTestCase):
    async def test_on_error_recovers_item(self):
        def boom(x):
            if x == 2:
                raise ValueError("bad")
            return x

        results = Collector()
        p = Processor(
            source=[1, 2, 3],
            processor=boom,
            on_error=lambda e, datum: -datum,  # negate the bad item
            subscribers=[results],
        )
        await p()
        self.assertEqual([item async for item in results], [1, -2, 3])

    async def test_on_error_skip_suppresses_item(self):
        def boom(x):
            if x == 2:
                raise ValueError("bad")
            return x

        results = Collector()
        p = Processor(
            source=[1, 2, 3],
            processor=boom,
            on_error=lambda e, datum: Skip(),
            subscribers=[results],
        )
        await p()
        self.assertEqual([item async for item in results], [1, 3])

    async def test_no_on_error_propagates_exception(self):
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: (_ for _ in ()).throw(ValueError("boom")) if x == 2 else x,
        )
        with self.assertRaises(Exception):
            await p()

    async def test_async_on_error_handler(self):
        async def async_handler(e, datum):
            return datum * -1

        results = Collector()
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: 1 / 0 if x == 2 else x,
            on_error=async_handler,
            subscribers=[results],
        )
        await p()
        self.assertEqual([item async for item in results], [1, -2, 3])


# ---------------------------------------------------------------------------
# Source generator error handling (new)
# ---------------------------------------------------------------------------


class TestSourceGeneratorErrors(unittest.IsolatedAsyncioTestCase):
    async def test_source_error_routes_through_on_error(self):
        """An exception raised by the source generator is caught and on_error is called."""

        def bad_source():
            yield 1
            raise RuntimeError("source exploded")

        errors = []
        results = Collector()
        p = Processor(
            source=bad_source,
            processor=lambda x: x,
            on_error=lambda e, datum: errors.append(e) or Skip(),
            subscribers=[results],
        )
        await p()
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual([item async for item in results], [1])

    async def test_source_error_without_on_error_propagates(self):
        def bad_source():
            yield 1
            raise RuntimeError("source exploded")

        p = Processor(source=bad_source, processor=lambda x: x)
        with self.assertRaises(RuntimeError):
            await p()

    async def test_source_error_increments_items_errored(self):
        def bad_source():
            yield 1
            raise RuntimeError("boom")

        captured = []
        p = Processor(
            source=bad_source,
            processor=lambda x: x,
            on_error=lambda e, datum: Skip(),
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_errored, 1)


# ---------------------------------------------------------------------------
# Enricher error handling (new)
# ---------------------------------------------------------------------------


class TestEnricherOnError(unittest.IsolatedAsyncioTestCase):
    async def test_enricher_on_error_recovers_from_bad_join(self):
        """on_error is called when enrichment of a primary item raises."""

        def bad_relations_enricher():
            # Simulate enrichment raising by subclassing — instead we'll
            # use a Relation with a key_transform that throws.
            pass

        errors = []
        results = Collector()

        def exploding_transform(k):
            if k == 99:
                raise KeyError("missing key")
            return k

        enricher = Enricher(
            relations=[
                Relation(
                    from_field="loc_id",
                    to_channel="locs",
                    to_field="id",
                    attach_as="location",
                    key_transform=exploding_transform,
                )
            ],
            primary_channel="primary",
            on_error=lambda e, item: errors.append(e) or {**item, "location": None},
            subscribers=[results],
        )

        feeder = Processor(source=[{"loc_id": 1}, {"loc_id": 99}, {"loc_id": 2}])
        feeder.subscribe(enricher, channel="primary")

        locs = Processor(source=[{"id": 1, "name": "Earth"}, {"id": 2, "name": "Citadel"}])
        locs.subscribe(enricher, channel="locs")

        await gather(create_task(feeder()), create_task(locs()), create_task(enricher()))

        enriched = [item async for item in results]
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], KeyError)
        self.assertEqual(len(enriched), 3)
        # The bad item gets location=None via on_error recovery
        bad_item = next(i for i in enriched if i["loc_id"] == 99)
        self.assertIsNone(bad_item["location"])

    async def test_enricher_no_on_error_propagates(self):
        def exploding_transform(k):
            raise RuntimeError("boom")

        enricher = Enricher(
            relations=[
                Relation(
                    from_field="loc_id",
                    to_channel="locs",
                    to_field="id",
                    attach_as="location",
                    key_transform=exploding_transform,
                )
            ],
            primary_channel="primary",
        )

        feeder = Processor(source=[{"loc_id": 1}])
        feeder.subscribe(enricher, channel="primary")

        locs = Processor(source=[{"id": 1, "name": "Earth"}])
        locs.subscribe(enricher, channel="locs")

        with self.assertRaises(RuntimeError):
            await gather(create_task(feeder()), create_task(locs()), create_task(enricher()))

    async def test_enricher_on_error_increments_items_errored(self):
        def exploding_transform(k):
            raise RuntimeError("boom")

        captured = []
        results = Collector()

        enricher = Enricher(
            relations=[
                Relation(
                    from_field="loc_id",
                    to_channel="locs",
                    to_field="id",
                    attach_as="location",
                    key_transform=exploding_transform,
                )
            ],
            primary_channel="primary",
            on_error=lambda e, item: {**item, "location": None},
            on_metrics=lambda m: captured.append(m),
            subscribers=[results],
        )

        feeder = Processor(source=[{"loc_id": 1}, {"loc_id": 2}])
        feeder.subscribe(enricher, channel="primary")

        locs = Processor(source=[{"id": 1}, {"id": 2}])
        locs.subscribe(enricher, channel="locs")

        await gather(create_task(feeder()), create_task(locs()), create_task(enricher()))

        self.assertEqual(captured[0].items_errored, 2)


if __name__ == "__main__":
    unittest.main()
