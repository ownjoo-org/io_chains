"""
UA test: same concurrent fetch → join → enrich → limit pipeline as
test_rick_and_morty_complex.py, but using the new declarative constructs:
  - subscribe(subscriber, channel=...) for channel-tagged fan-in
  - Enricher + Relation for the join/enrich phase
  - Limit(n) to cap output

Topology:
    fetch_characters ─ subscribe(enricher, channel='chars')  ─┐
    fetch_episodes   ─ subscribe(enricher, channel='episodes')─┤ Enricher
    fetch_locations  ─ subscribe(enricher, channel='locations')┘
                                   ↓
                          Limit(2) as processor
                                   ↓
                               results Collector
"""

import unittest
from asyncio import create_task, gather

from httpx import AsyncClient

from io_chains.links.enricher import Enricher, Relation
from io_chains.links.processor import Processor
from test.helpers.limit import Limit
from io_chains.links.collector import Collector

BASE_URL = "https://rickandmortyapi.com/api"


async def fetch_characters_page(client: AsyncClient):
    response = await client.get(f"{BASE_URL}/character")
    response.raise_for_status()
    for character in response.json()["results"]:
        yield character


async def fetch_episodes_page(client: AsyncClient):
    response = await client.get(f"{BASE_URL}/episode")
    response.raise_for_status()
    for episode in response.json()["results"]:
        yield episode


async def fetch_locations_page(client: AsyncClient):
    response = await client.get(f"{BASE_URL}/location")
    response.raise_for_status()
    for location in response.json()["results"]:
        yield location


def extract_id(url: str) -> int | None:
    try:
        return int(url.rstrip("/").split("/")[-1])
    except (ValueError, AttributeError, IndexError):
        return None


def normalize_episode_ids(char: dict) -> dict:
    """Add an episode_ids list (ints) derived from the episode URL list."""
    return {**char, "episode_ids": [extract_id(u) for u in char.get("episode", [])]}


def normalize_location_id(char: dict) -> dict:
    """Add a location_id int derived from the location URL."""
    return {**char, "location_id": extract_id(char.get("location", {}).get("url", ""))}


class TestEnricherPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_enricher_pipeline_first_two_characters(self):
        """
        Demonstrates the declarative enrichment pipeline.

        Characters arrive in their raw API form. Two normalization Processors add
        integer FK fields (location_id, episode_ids) needed by the Relations.
        The Enricher collects all side channels then enriches and streams primaries.
        Limit(2) caps the output to the first two characters.
        """
        async with AsyncClient() as client:
            results = Collector()

            # Relations: describe how to join character ↔ location and character ↔ episodes
            relations = [
                Relation(
                    from_field="location_id",
                    to_channel="locations",
                    to_field="id",
                    attach_as="location_detail",
                ),
                Relation(
                    from_field="episode_ids",
                    to_channel="episodes",
                    to_field="id",
                    attach_as="episode_details",
                    many=True,
                ),
            ]

            # Enricher is the fan-in hub; output is limited to first 2
            enricher = Enricher(
                relations=relations,
                primary_channel="chars",
                subscribers=[Processor(processor=Limit(2), subscribers=[results])],
            )

            # Normalize character FK fields before they reach the Enricher
            char_normalizer = Processor(
                source=fetch_characters_page(client),
                processor=lambda c: normalize_location_id(normalize_episode_ids(c)),
            )
            char_normalizer.subscribe(enricher, channel="chars")

            episode_link = Processor(source=fetch_episodes_page(client))
            episode_link.subscribe(enricher, channel="episodes")

            location_link = Processor(source=fetch_locations_page(client))
            location_link.subscribe(enricher, channel="locations")

            limit_link = enricher._subscribers[0]  # the Limit processor

            await gather(
                create_task(char_normalizer()),
                create_task(episode_link()),
                create_task(location_link()),
                create_task(enricher()),
                create_task(limit_link()),
            )

        enriched = [item async for item in results]

        # --- Assertions ---
        self.assertEqual(2, len(enriched))

        rick, morty = enriched[0], enriched[1]
        self.assertEqual("Rick Sanchez", rick["name"])
        self.assertEqual("Morty Smith", morty["name"])

        for char in enriched:
            self.assertIsNotNone(char["location_detail"])
            self.assertIn("name", char["location_detail"])
            self.assertIn("dimension", char["location_detail"])

            self.assertGreater(len(char["episode_details"]), 0)
            for ep in char["episode_details"]:
                self.assertIn("name", ep)
                self.assertIn("episode", ep)
                self.assertIn("air_date", ep)

        self.assertEqual("S01E01", rick["episode_details"][0]["episode"])


if __name__ == "__main__":
    unittest.main()
