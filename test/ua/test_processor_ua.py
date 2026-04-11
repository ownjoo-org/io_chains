import unittest
from collections.abc import AsyncGenerator
from logging import getLogger

from httpx import AsyncClient, Response

from io_chains.links.processor import Processor
from io_chains.links.collector import Collector

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


class TestProcessor(unittest.IsolatedAsyncioTestCase):
    async def test_processor_should_extract_rick_and_morty(self):
        results = Collector()

        pipeline = Processor(
            source=get_rick_and_morty,
            processor=get_json,
            subscribers=[
                results,
                Processor(processor=lambda value: print(f"API response: {list(value.keys())}") or value),
            ],
        )

        await pipeline()

        actual = [item async for item in results]
        self.assertEqual(1, len(actual))
        self.assertIsInstance(actual[0], dict)


if __name__ == "__main__":
    unittest.main()
