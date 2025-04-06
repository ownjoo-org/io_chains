from typing import Generator

from requests import Response, get

from linkables.extract_link import ExtractLink
from linkables.link import Link
from subscribables.subscriber import Subscriber


def get_rick_and_morty() -> Generator[Response, None, None]:
    response: Response = get(url='https://rickandmortyapi.com/api/')
    yield response


def main():
    # prepare to show the headers
    headers_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.headers=}\n\n',  # extract what we need and publish
        subscribers=[
            Subscriber(callback=lambda value: print(value)),  # print subscriber just so we can see some output
        ],
    )

    # prepare to show the body
    json_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.json()=}\n\n',
        subscribers=Subscriber(callback=lambda value: print(value)),
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

    ExtractLink(
        in_iter=[0, 1, 2],
        subscribers=[
            Subscriber(callback=lambda value: print(f'LIST VAL: {value}')),
        ],
    )()

    ExtractLink(
        in_iter=range(10),
        subscribers=[
            Subscriber(callback=lambda value: print(f'GEN VAL: {value}')),
        ],
    )()


if __name__ == '__main__':
    main()
