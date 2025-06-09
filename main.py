from typing import Generator

from requests import Response, get

from io_chains.linkables.extract_link import ExtractLink
from io_chains.linkables.link import Link
from io_chains.subscribables.callbacksubscriber import CallbackSubscriber


def get_rick_and_morty() -> Generator[Response, None, None]:
    response: Response = get(url='https://rickandmortyapi.com/api/')
    yield response


def main():
    # prepare to show the headers
    headers_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.headers=}\n\n',  # extract what we need and publish
        subscribers=[
            CallbackSubscriber(callback=lambda value: print(value)),  # print subscriber just so we can see some output
        ],
    )

    # prepare to show the body
    json_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.json()=}\n\n',
        subscribers=CallbackSubscriber(callback=lambda value: print(value)),
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
    rick_and_morty_extractor()

    # example starting with a list
    ExtractLink(
        in_iter=[0, 1, 2],
        subscribers=[
            lambda value: print(f'LIST VAL: {value}'),
        ],
    )()

    # example starting with a generator
    ExtractLink(
        in_iter=range(10),
        subscribers=[
            CallbackSubscriber(callback=lambda value: print(f'GEN VAL: {value}')),
        ],
    )()


if __name__ == '__main__':
    main()
