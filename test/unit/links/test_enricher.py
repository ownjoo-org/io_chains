"""
Tests for Enricher, Relation, Envelope, ChannelSubscriber, and Limit.

Design intent:
  - subscribe(subscriber, channel=None) on Link wires a ChannelSubscriber adapter
    so items arrive at the subscriber tagged with which channel they came from.
  - Envelope(data, channel) is the lightweight wrapper emitted by ChannelSubscriber.
  - Relation describes a foreign-key-style join: which field on the primary item
    maps to which field on related items (from a named channel).
  - Enricher is a Linkable that collects all side-channel data first, then
    streams primary items enriched according to a list of Relations.
  - Limit(n) is a stateful callable that passes through the first n items and
    returns SKIP for all subsequent ones.
"""
import unittest
from asyncio import create_task, gather

from io_chains.links.enricher import Enricher, Relation
from io_chains.links.limit import Limit
from io_chains.links.link import Link
from io_chains.pubsub.collector import Collector
from io_chains.pubsub.envelope import Envelope
from io_chains.pubsub.sentinel import SKIP

# ---------------------------------------------------------------------------
# Limit
# ---------------------------------------------------------------------------

class TestLimit(unittest.IsolatedAsyncioTestCase):

    async def test_limit_passes_first_n_items(self):
        results = Collector()
        link = Link(source=list(range(10)), transformer=Limit(3), subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 1, 2], actual)

    async def test_limit_of_zero_skips_all(self):
        results = Collector()
        link = Link(source=[1, 2, 3], transformer=Limit(0), subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([], actual)

    async def test_limit_larger_than_source_passes_all(self):
        results = Collector()
        link = Link(source=[1, 2, 3], transformer=Limit(100), subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 2, 3], actual)

    def test_limit_returns_skip_sentinel(self):
        lim = Limit(1)
        self.assertEqual(99, lim(99))   # first call: pass through
        self.assertIs(SKIP, lim(99))    # second call: SKIP


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------

class TestEnvelope(unittest.IsolatedAsyncioTestCase):

    def test_envelope_stores_data_and_channel(self):
        env = Envelope(data={'id': 1}, channel='chars')
        self.assertEqual({'id': 1}, env.data)
        self.assertEqual('chars', env.channel)

    def test_envelope_equality(self):
        a = Envelope(data=42, channel='x')
        b = Envelope(data=42, channel='x')
        self.assertEqual(a, b)

    def test_envelope_inequality_different_channel(self):
        a = Envelope(data=42, channel='x')
        b = Envelope(data=42, channel='y')
        self.assertNotEqual(a, b)

    def test_envelope_repr_is_readable(self):
        env = Envelope(data='hello', channel='greet')
        self.assertIn('greet', repr(env))
        self.assertIn('hello', repr(env))


# ---------------------------------------------------------------------------
# subscribe() + ChannelSubscriber
# ---------------------------------------------------------------------------

class TestSubscribeWithChannel(unittest.IsolatedAsyncioTestCase):

    async def test_subscribe_without_channel_behaves_like_normal_subscriber(self):
        """subscribe(sub) with no channel should just wire sub directly."""
        results = Collector()
        link = Link(source=[1, 2, 3])
        link.subscribe(results)
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 2, 3], actual)

    async def test_subscribe_with_channel_wraps_items_in_envelope(self):
        results = Collector()
        link = Link(source=[10, 20])
        link.subscribe(results, channel='nums')
        await link()
        actual = [item async for item in results]
        self.assertEqual(2, len(actual))
        self.assertIsInstance(actual[0], Envelope)
        self.assertEqual('nums', actual[0].channel)
        self.assertEqual(10, actual[0].data)
        self.assertEqual(20, actual[1].data)

    async def test_subscribe_multiple_channels_to_same_downstream(self):
        """Two upstream Links publish to the same downstream with different channel tags."""
        downstream = Collector()
        link_a = Link(source=['a1', 'a2'])
        link_b = Link(source=['b1'])
        link_a.subscribe(downstream, channel='A')
        link_b.subscribe(downstream, channel='B')
        await gather(create_task(link_a()), create_task(link_b()))
        items = [item async for item in downstream]
        channels = {e.channel for e in items}
        self.assertEqual({'A', 'B'}, channels)
        data_by_channel = {}
        for e in items:
            data_by_channel.setdefault(e.channel, []).append(e.data)
        self.assertEqual(['a1', 'a2'], sorted(data_by_channel['A']))
        self.assertEqual(['b1'], data_by_channel['B'])


# ---------------------------------------------------------------------------
# Relation
# ---------------------------------------------------------------------------

class TestRelation(unittest.IsolatedAsyncioTestCase):

    def test_relation_stores_fields(self):
        rel = Relation(
            from_field='location_id',
            to_channel='locations',
            to_field='id',
            attach_as='location_detail',
        )
        self.assertEqual('location_id', rel.from_field)
        self.assertEqual('locations', rel.to_channel)
        self.assertEqual('id', rel.to_field)
        self.assertEqual('location_detail', rel.attach_as)
        self.assertFalse(rel.many)

    def test_relation_many_flag(self):
        rel = Relation(
            from_field='episode_ids',
            to_channel='episodes',
            to_field='id',
            attach_as='episode_details',
            many=True,
        )
        self.assertTrue(rel.many)


# ---------------------------------------------------------------------------
# Enricher
# ---------------------------------------------------------------------------

CHARS = [
    {'id': 1, 'name': 'Rick', 'location_id': 20, 'episode_ids': [1, 2]},
    {'id': 2, 'name': 'Morty', 'location_id': 3, 'episode_ids': [1]},
]

LOCATIONS = [
    {'id': 3, 'name': 'Earth'},
    {'id': 20, 'name': 'Citadel'},
]

EPISODES = [
    {'id': 1, 'name': 'Pilot'},
    {'id': 2, 'name': 'Lawnmower Dog'},
]


class TestEnricher(unittest.IsolatedAsyncioTestCase):

    async def _run_enricher(self, chars, locations, episodes, relations, limit=None):
        """Helper: wire chars/locations/episodes into an Enricher and collect results."""
        results = Collector()
        enricher = Enricher(relations=relations, primary_channel='chars', subscribers=[results])

        chars_link = Link(source=chars)
        chars_link.subscribe(enricher, channel='chars')

        loc_link = Link(source=locations)
        loc_link.subscribe(enricher, channel='locations')

        ep_link = Link(source=episodes)
        ep_link.subscribe(enricher, channel='episodes')

        await gather(
            create_task(chars_link()),
            create_task(loc_link()),
            create_task(ep_link()),
            create_task(enricher()),
        )
        return [item async for item in results]

    async def test_enricher_one_to_one_relation(self):
        relations = [
            Relation(from_field='location_id', to_channel='locations',
                     to_field='id', attach_as='location_detail'),
        ]
        enriched = await self._run_enricher(CHARS, LOCATIONS, EPISODES, relations)
        self.assertEqual(2, len(enriched))
        rick = next(c for c in enriched if c['name'] == 'Rick')
        self.assertEqual({'id': 20, 'name': 'Citadel'}, rick['location_detail'])

    async def test_enricher_one_to_many_relation(self):
        relations = [
            Relation(from_field='episode_ids', to_channel='episodes',
                     to_field='id', attach_as='episode_details', many=True),
        ]
        enriched = await self._run_enricher(CHARS, LOCATIONS, EPISODES, relations)
        rick = next(c for c in enriched if c['name'] == 'Rick')
        self.assertEqual(2, len(rick['episode_details']))
        ep_names = {e['name'] for e in rick['episode_details']}
        self.assertEqual({'Pilot', 'Lawnmower Dog'}, ep_names)

    async def test_enricher_multiple_relations(self):
        relations = [
            Relation(from_field='location_id', to_channel='locations',
                     to_field='id', attach_as='location_detail'),
            Relation(from_field='episode_ids', to_channel='episodes',
                     to_field='id', attach_as='episode_details', many=True),
        ]
        enriched = await self._run_enricher(CHARS, LOCATIONS, EPISODES, relations)
        morty = next(c for c in enriched if c['name'] == 'Morty')
        self.assertEqual({'id': 3, 'name': 'Earth'}, morty['location_detail'])
        self.assertEqual([{'id': 1, 'name': 'Pilot'}], morty['episode_details'])

    async def test_enricher_unmatched_relation_attaches_none(self):
        """If no related item matches, attach_as field should be None (one-to-one)."""
        chars_with_missing = [{'id': 99, 'name': 'Unknown', 'location_id': 999, 'episode_ids': []}]
        relations = [
            Relation(from_field='location_id', to_channel='locations',
                     to_field='id', attach_as='location_detail'),
        ]
        enriched = await self._run_enricher(chars_with_missing, LOCATIONS, EPISODES, relations)
        self.assertIsNone(enriched[0]['location_detail'])

    async def test_enricher_unmatched_many_relation_attaches_empty_list(self):
        chars_no_eps = [{'id': 5, 'name': 'Beth', 'location_id': 3, 'episode_ids': [99]}]
        relations = [
            Relation(from_field='episode_ids', to_channel='episodes',
                     to_field='id', attach_as='episode_details', many=True),
        ]
        enriched = await self._run_enricher(chars_no_eps, LOCATIONS, EPISODES, relations)
        self.assertEqual([], enriched[0]['episode_details'])

    async def test_enricher_streams_primary_items_in_order(self):
        relations = [
            Relation(from_field='location_id', to_channel='locations',
                     to_field='id', attach_as='location_detail'),
        ]
        enriched = await self._run_enricher(CHARS, LOCATIONS, EPISODES, relations)
        names = [c['name'] for c in enriched]
        self.assertEqual(['Rick', 'Morty'], names)


if __name__ == '__main__':
    unittest.main()
