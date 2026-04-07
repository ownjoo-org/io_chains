"""
Complex pipeline UA test: concurrent fetch → join → enrich → limit.

Topology:
    fetch_characters ─┐
    fetch_episodes   ─┤  (concurrent, Phase 1)
    fetch_locations  ─┘
                       ↓
                   [join: wait for all three]
                       ↓
                   enrich_characters  (Phase 2)
                       ↓
                   first 2 results

This test intentionally uses the raw current API so we can see exactly
what is verbose and needs to be simplified for declarative construction.
"""
import unittest
from asyncio import create_task, gather

from httpx import AsyncClient

from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector

BASE_URL = 'https://rickandmortyapi.com/api'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_id(url: str) -> int | None:
    """Extract the numeric ID from a Rick and Morty API URL."""
    try:
        return int(url.rstrip('/').split('/')[-1])
    except (ValueError, AttributeError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Async generator sources  (each page = 20 records)
# ---------------------------------------------------------------------------

async def fetch_characters_page(client: AsyncClient):
    response = await client.get(f'{BASE_URL}/character')
    response.raise_for_status()
    for character in response.json()['results']:
        yield character


async def fetch_episodes_page(client: AsyncClient):
    response = await client.get(f'{BASE_URL}/episode')
    response.raise_for_status()
    for episode in response.json()['results']:
        yield episode


async def fetch_locations_page(client: AsyncClient):
    response = await client.get(f'{BASE_URL}/location')
    response.raise_for_status()
    for location in response.json()['results']:
        yield location


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich_character(
    char: dict,
    episode_lookup: dict[int, dict],
    location_lookup: dict[int, dict],
) -> dict:
    loc_id = extract_id(char.get('location', {}).get('url', ''))
    ep_ids = [extract_id(url) for url in char.get('episode', [])]
    return {
        **char,
        'location_detail': location_lookup.get(loc_id),
        'episode_details': [episode_lookup[i] for i in ep_ids if i in episode_lookup],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConcurrentFetchJoinAndEnrich(unittest.IsolatedAsyncioTestCase):

    async def test_concurrent_fetch_join_and_enrich_first_two(self):
        """
        Phase 1 — three Links run concurrently, each draining into a Collector:
            fetch_characters | fetch_episodes | fetch_locations

        Phase 2 — after ALL three complete (the join):
            build lookup dicts, enrich first 2 characters, collect results.

        Observations (what the current API makes us do manually):
          - Three Collectors just to buffer intermediate data
          - Explicit gather + create_task for the concurrent phase
          - Manual drain of each Collector into a lookup structure
          - The "join" is an implicit gap between two awaits, not a named construct
          - Limiting to N results requires slicing outside the pipeline
        """
        async with AsyncClient() as client:

            # --- Phase 1: concurrent fetches ---
            char_sink = Collector()
            episode_sink = Collector()
            location_sink = Collector()

            await gather(
                create_task(Link(source=fetch_characters_page(client), subscribers=[char_sink])()),
                create_task(Link(source=fetch_episodes_page(client), subscribers=[episode_sink])()),
                create_task(Link(source=fetch_locations_page(client), subscribers=[location_sink])()),
            )

            # --- Join: drain collectors into lookup structures ---
            chars = [c async for c in char_sink]
            episode_lookup = {e['id']: e async for e in episode_sink}
            location_lookup = {l['id']: l async for l in location_sink}

            # --- Phase 2: enrich first 2 characters ---
            results = Collector()
            await Link(
                source=chars[:2],
                transformer=lambda c: enrich_character(c, episode_lookup, location_lookup),
                subscribers=[results],
            )()

            enriched = [r async for r in results]

        # --- Assertions ---
        self.assertEqual(2, len(enriched))

        rick, morty = enriched[0], enriched[1]
        self.assertEqual('Rick Sanchez', rick['name'])
        self.assertEqual('Morty Smith', morty['name'])

        for char in enriched:
            # Location enriched (both Rick and Morty's locations are on page 1)
            self.assertIsNotNone(char['location_detail'])
            self.assertIn('name', char['location_detail'])
            self.assertIn('dimension', char['location_detail'])

            # Episodes enriched (their first episodes are on page 1)
            self.assertGreater(len(char['episode_details']), 0)
            for ep in char['episode_details']:
                self.assertIn('name', ep)
                self.assertIn('episode', ep)   # e.g. "S01E01"
                self.assertIn('air_date', ep)

        # Spot-check Rick: first episode should be S01E01
        self.assertEqual('S01E01', rick['episode_details'][0]['episode'])


if __name__ == '__main__':
    unittest.main()
