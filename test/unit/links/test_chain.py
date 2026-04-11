"""
User acceptance tests for Chain.

Design principles under test:
  - A Link is a single processing unit. Knows only its immediate neighbors.
  - A Chain is the orchestrator. Contains Links or other Chains. Manages
    their concurrent execution internally so callers don't need gather().
  - A Chain is itself a Link: it has an input face (first link) and an
    output face (last link), and can be connected to other Links or Chains.
  - When a Chain is a standalone pipeline, await chain() is all you need.
  - When a Chain is a subscriber of an external Link, the caller still needs
    to run both — but Chain manages its own internals.

Limitations intentionally deferred:
  - Fan-in (multiple upstream sources into one downstream link)
"""

import unittest
from asyncio import create_task, gather
from collections.abc import AsyncGenerator

from io_chains.links.chain import Chain
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector

# ---------------------------------------------------------------------------
# Simulated async data sources
# ---------------------------------------------------------------------------


async def source_records() -> AsyncGenerator[dict, None]:
    for record in [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Carol"},
    ]:
        yield record


async def enrich_with_score(record: dict) -> dict:
    scores = {1: 95, 2: 87, 3: 72}
    return {**record, "score": scores.get(record["id"], 0)}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestChain(unittest.IsolatedAsyncioTestCase):
    # --- Instantiation ---

    def test_chain_should_instantiate(self):
        chain = Chain(
            links=[
                Processor(processor=lambda x: x * 2),
                Processor(processor=lambda x: x + 1),
            ]
        )
        self.assertIsInstance(chain, Chain)

    def test_chain_should_accept_links_and_chains(self):
        # A Chain can contain Processors or other Chains as its elements
        inner = Chain(links=[Processor(processor=lambda x: x * 2)])
        outer = Chain(
            links=[
                Processor(processor=lambda x: x + 1),
                inner,
            ]
        )
        self.assertIsInstance(outer, Chain)

    # --- Chain as standalone orchestrator ---

    async def test_chain_runs_without_caller_managing_gather(self):
        # The fundamental Chain promise: caller just awaits chain(), no gather needed
        results = Collector()

        chain = Chain(
            source=[1, 2, 3],
            links=[
                Processor(processor=lambda x: x * 2),
                Processor(processor=lambda x: x + 10),
            ],
            subscribers=[results],
        )

        await chain()  # no gather, no create_task

        actual = [item async for item in results]
        self.assertEqual(actual, [12, 14, 16])

    async def test_long_chain_of_links(self):
        # Many sequential stages, caller still just awaits chain()
        results = Collector()

        chain = Chain(
            source=[1, 2, 3],
            links=[
                Processor(processor=lambda x: x + 1),  # 2, 3, 4
                Processor(processor=lambda x: x * 3),  # 6, 9, 12
                Processor(processor=lambda x: x - 1),  # 5, 8, 11
                Processor(processor=lambda x: x * 2),  # 10, 16, 22
                Processor(processor=str),               # '10', '16', '22'
            ],
            subscribers=[results],
        )

        await chain()

        actual = [item async for item in results]
        self.assertEqual(actual, ["10", "16", "22"])

    async def test_chain_fan_out_last_link_to_multiple_subscribers(self):
        # The last link in a Chain can publish to multiple subscribers
        sink1 = Collector()
        sink2 = Collector()
        sink3 = Collector()

        chain = Chain(
            source=[1, 2, 3],
            links=[
                Processor(processor=lambda x: x * 2),
            ],
            subscribers=[sink1, sink2, sink3],
        )

        await chain()

        self.assertEqual([item async for item in sink1], [2, 4, 6])
        self.assertEqual([item async for item in sink2], [2, 4, 6])
        self.assertEqual([item async for item in sink3], [2, 4, 6])

    # --- Chain containing Chains ---

    async def test_chain_of_chains(self):
        # A Chain whose elements are themselves Chains
        results = Collector()

        normalise = Chain(
            links=[
                Processor(processor=lambda x: abs(x)),
                Processor(processor=lambda x: round(x, 2)),
            ]
        )

        stringify = Chain(
            links=[
                Processor(processor=lambda x: x * 100),
                Processor(processor=lambda x: f"{x:.0f}%"),
            ]
        )

        pipeline = Chain(
            source=[-0.156, 0.999, -0.301],
            links=[normalise, stringify],
            subscribers=[results],
        )

        await pipeline()

        actual = [item async for item in results]
        self.assertEqual(actual, ["16%", "100%", "30%"])

    async def test_mixed_chain_of_links_and_chains(self):
        # A Chain containing a mix of Processors and sub-Chains
        results = Collector()

        middle_chain = Chain(
            links=[
                Processor(processor=lambda x: x * 10),
                Processor(processor=lambda x: x - 1),
            ]
        )

        pipeline = Chain(
            source=[1, 2, 3],
            links=[
                Processor(processor=lambda x: x + 1),  # 2, 3, 4
                middle_chain,                           # *10-1: 19, 29, 39
                Processor(processor=str),               # '19', '29', '39'
            ],
            subscribers=[results],
        )

        await pipeline()

        actual = [item async for item in results]
        self.assertEqual(actual, ["19", "29", "39"])

    # --- Chain as Link (connectable to other Links/Chains) ---

    async def test_chain_as_subscriber_of_link(self):
        # An external Link feeds into a Chain.
        # Caller runs both, but Chain manages its own internals.
        results = Collector()

        transform_chain = Chain(
            links=[
                Processor(processor=lambda x: x * 2),
                Processor(processor=lambda x: x + 100),
            ],
            subscribers=[results],
        )

        source = Processor(
            source=[1, 2, 3],
            subscribers=[transform_chain],
        )

        # Caller runs source and chain — but not chain's individual links
        await gather(
            create_task(source()),
            create_task(transform_chain()),
        )

        actual = [item async for item in results]
        self.assertEqual(actual, [102, 104, 106])

    async def test_chain_as_subscriber_of_chain(self):
        # Two Chains connected together — outer caller manages only the two chains
        results = Collector()

        chain_b = Chain(
            links=[
                Processor(processor=lambda x: x * 3),
            ],
            subscribers=[results],
        )

        chain_a = Chain(
            source=[1, 2, 3],
            links=[
                Processor(processor=lambda x: x + 1),
            ],
            subscribers=[chain_b],
        )

        await gather(
            create_task(chain_a()),
            create_task(chain_b()),
        )

        actual = [item async for item in results]
        self.assertEqual(actual, [6, 9, 12])

    # --- Realistic ETL patterns ---

    async def test_etl_fetch_transform_enrich(self):
        # fetch → transform → enrich, all managed by a single Chain
        results = Collector()

        pipeline = Chain(
            source=source_records,
            links=[
                Processor(processor=lambda r: {**r, "name": r["name"].upper()}),
                Processor(processor=enrich_with_score),
            ],
            subscribers=[results],
        )

        await pipeline()

        actual = [item async for item in results]
        self.assertEqual(len(actual), 3)
        self.assertEqual(actual[0], {"id": 1, "name": "ALICE", "score": 95})
        self.assertEqual(actual[1], {"id": 2, "name": "BOB", "score": 87})
        self.assertEqual(actual[2], {"id": 3, "name": "CAROL", "score": 72})

    async def test_reusable_chain_embedded_in_larger_pipeline(self):
        # A packaged sub-pipeline (Chain) reused inside a larger flow.
        results = Collector()

        normalise_records = Chain(
            links=[
                Processor(processor=lambda r: {**r, "name": r["name"].strip().title()}),
                Processor(processor=enrich_with_score),
            ]
        )

        full_pipeline = Chain(
            source=source_records,
            links=[
                normalise_records,
                Processor(processor=lambda r: f"{r['name']} ({r['score']})"),
            ],
            subscribers=[results],
        )

        await full_pipeline()

        actual = [item async for item in results]
        self.assertEqual(actual, ["Alice (95)", "Bob (87)", "Carol (72)"])


if __name__ == "__main__":
    unittest.main()
