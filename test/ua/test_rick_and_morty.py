"""
UA test: Rick and Morty character enrichment pipeline.

Demonstrates a real-world io_chains use case:
  - Fetch specific characters from a REST API (async generator source)
  - Enrich each character with their current location details (second API call)
  - Enrich each character with their episode details (parallel API calls)
  - Collect fully-enriched character dicts at the end of the chain

A single shared AsyncClient is used across all enrichment calls to allow
connection pooling and avoid rate limiting.

This is an integration test: it makes real HTTP calls to rickandmortyapi.com.
"""

import unittest
from asyncio import gather
from collections.abc import AsyncGenerator

from httpx import AsyncClient

from io_chains.links.chain import Chain
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector

BASE_URL = "https://rickandmortyapi.com/api"
MAX_EPISODES_PER_CHARACTER = 3
CHARACTER_IDS = [1, 2, 3, 4, 5]  # Rick, Morty, and three others


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


async def fetch_characters() -> AsyncGenerator[dict, None]:
    """Fetch specific characters by ID in a single request, yield each individually."""
    ids = ",".join(str(i) for i in CHARACTER_IDS)
    async with AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/character/{ids}")
        response.raise_for_status()
        for character in response.json():
            yield character


# ---------------------------------------------------------------------------
# Pipeline factory — builds processors sharing one client
# ---------------------------------------------------------------------------


def build_pipeline(results: Collector, client: AsyncClient) -> Chain:
    """
    Build the enrichment pipeline with a shared HTTP client.

    Chain structure:
        fetch_characters → enrich_with_location → enrich_with_episodes → results
    """

    async def enrich_with_location(character: dict) -> dict:
        url = character.get("location", {}).get("url")
        if not url:
            return {**character, "location_details": None}
        response = await client.get(url)
        response.raise_for_status()
        return {**character, "location_details": response.json()}

    async def enrich_with_episodes(character: dict) -> dict:
        episode_urls = character.get("episode", [])[:MAX_EPISODES_PER_CHARACTER]
        if not episode_urls:
            return {**character, "episode_details": []}
        responses = await gather(*[client.get(url) for url in episode_urls])
        return {**character, "episode_details": [r.json() for r in responses]}

    return Chain(
        source=fetch_characters,
        links=[
            Processor(processor=enrich_with_location),
            Processor(processor=enrich_with_episodes),
        ],
        subscribers=[results],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRickAndMortyEnrichmentPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_enriched_characters_have_location_and_episodes(self):
        results = Collector()

        async with AsyncClient() as client:
            pipeline = build_pipeline(results, client)
            await pipeline()

        characters = [char async for char in results]

        self.assertEqual(len(characters), len(CHARACTER_IDS))

        for char in characters:
            # Core character fields present
            self.assertIn("id", char)
            self.assertIn("name", char)
            self.assertIn("status", char)
            self.assertIn("species", char)

            # Location enrichment present
            self.assertIn("location_details", char)
            if char["location"]["url"]:
                location = char["location_details"]
                self.assertIn("name", location)
                self.assertIn("dimension", location)

            # Episode enrichment present
            self.assertIn("episode_details", char)
            for episode in char["episode_details"]:
                self.assertIn("name", episode)
                self.assertIn("episode", episode)  # e.g. "S01E01"
                self.assertIn("air_date", episode)

        # Spot check: Rick Sanchez is character #1
        rick = characters[0]
        self.assertEqual(rick["name"], "Rick Sanchez")
        self.assertEqual(rick["status"], "Alive")
        self.assertEqual(len(rick["episode_details"]), MAX_EPISODES_PER_CHARACTER)
        self.assertEqual(rick["episode_details"][0]["episode"], "S01E01")

        # Spot check: Morty Smith is character #2
        morty = characters[1]
        self.assertEqual(morty["name"], "Morty Smith")
        self.assertEqual(len(morty["episode_details"]), MAX_EPISODES_PER_CHARACTER)


if __name__ == "__main__":
    unittest.main()
