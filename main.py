from asyncio import run
from logging import getLogger
from typing import AsyncGenerator

from aiohttp import ClientResponse, ClientSession
from httpx import Response
from io_chains.linkables.extract_link import ExtractLink
from io_chains.linkables.link import Link
from io_chains.subscribables.callbacksubscriber import CallbackSubscriber

logger = getLogger(__name__)


async def get_rick_and_morty() -> AsyncGenerator[Response, None]:
    try:
        async with ClientSession() as session:
            response: ClientResponse = await session.get(url='https://rickandmortyapi.com/api/')
            yield response
    except Exception as e:
        logger.exception(f'get_rick_and_morty: {e}')


async def get_json(resp, *args, **kwargs):
    logger.info(f'GET JSON RESPONSE: {resp}')
    data = await resp.json()
    return f'\n\n{data}\n\n'


def main():
    try:
        # prepare to show the headers
        headers_link = Link(
            processor=lambda resp, *args, **kwargs: f'HEADERS LINK SUB:\n{resp.headers}\n\n',
            subscribers=[
                CallbackSubscriber(callback=lambda value: print(value))  # print just so we can see some output
            ],
        )

        # prepare to show the body
        json_link = Link(
            processor=get_json,
            subscribers=CallbackSubscriber(callback=lambda value: print(f'JSON LINK SUB: {value}')),
        )

        # prepare to get some data and generate from the response (or just the whole response in this case)
        rick_and_morty_extractor: ExtractLink = ExtractLink(
            in_iter=get_rick_and_morty,
            subscribers=[
                headers_link,
                json_link,
            ],
        )

        # now that we've prepared the chain, make it go
        run(rick_and_morty_extractor())

        # example starting with a list
        run(
            ExtractLink(
                in_iter=[0, 1, 2],
                subscribers=[
                    lambda value: print(f'LIST VAL: {value}'),
                ],
            )()
        )

        # example starting with a generator
        run(
            ExtractLink(
                in_iter=range(10),
                subscribers=[
                    CallbackSubscriber(callback=lambda value: print(f'GEN VAL: {value}')),
                ],
            )()
        )
    except Exception as e:
        logger.exception(f'main: {e}')


if __name__ == '__main__':
    main()
