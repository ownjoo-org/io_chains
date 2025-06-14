import unittest
from collections.abc import AsyncGenerator
from logging import getLogger

from aiohttp import ClientResponse, ClientSession
from httpx import Response
from io_chains.linkables.link import Link
from io_chains.subscribables.callback_subscriber import CallbackSubscriber

logger = getLogger()


async def get_rick_and_morty() -> AsyncGenerator[Response, None]:
    try:
        async with ClientSession() as session:
            response: ClientResponse = await session.get(url='https://rickandmortyapi.com/api/')
            yield response
    except Exception as e:
        logger.exception(f'get_rick_and_morty: {e}')


async def get_json(resp, *args, **kwargs):
    return await resp.json()


class TestLink(unittest.IsolatedAsyncioTestCase):
    async def test_link_should_extract_rick_and_morty(self):
        # setup
        # prepare to show the headers
        headers_link = Link(
            transformer=lambda resp, *args, **kwargs: f'HEADERS LINK SUB:\n{resp.headers}\n\n',
            subscribers=[
                CallbackSubscriber(callback=lambda value: print(value))  # print just so we can see some output
            ],
        )

        # prepare to show the body
        json_link = Link(
            transformer=get_json,
            subscribers=CallbackSubscriber(callback=lambda value: print(f'JSON LINK SUB: {value}')),
        )

        # prepare to get some data and generate from the response (or just the whole response in this case)
        rick_and_morty_extractor: Link = Link(
            in_iter=get_rick_and_morty,
            subscribers=[
                headers_link,
                json_link,
            ],
        )

        # execute
        actual = await rick_and_morty_extractor()

        # assess
        self.assertIsInstance(rick_and_morty_extractor, Link)
        # self.assertIsInstance(actual, AsyncGenerator)

        # teardown


if __name__ == '__main__':
    unittest.main()
