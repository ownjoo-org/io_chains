import unittest
from collections.abc import AsyncGenerator
from logging import getLogger

from httpx import AsyncClient, Response

from io_chains.links.link import Link
from io_chains.pubsub.callback_subscriber import CallbackSubscriber
from io_chains.pubsub.collector import Collector

logger = getLogger()


async def get_rick_and_morty() -> AsyncGenerator[Response, None]:
    try:
        async with AsyncClient() as session:
            response: Response = await session.get(url="https://rickandmortyapi.com/api/")
            yield response
    except Exception as e:
        logger.exception(f"get_rick_and_morty: {e}")


async def get_json(resp):
    return await resp.json()


class TestLink(unittest.IsolatedAsyncioTestCase):
    async def test_link_should_extract_rick_and_morty(self):
        results = Collector()

        pipeline = Link(
            source=get_rick_and_morty,
            transformer=get_json,
            subscribers=[
                results,
                CallbackSubscriber(callback=lambda value: print(f"API response: {list(value.keys())}")),
            ],
        )

        await pipeline()

        actual = [item async for item in results]
        self.assertEqual(1, len(actual))
        self.assertIsInstance(actual[0], dict)


if __name__ == "__main__":
    unittest.main()
