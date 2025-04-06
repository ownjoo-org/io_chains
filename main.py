from typing import Generator

from requests import Response, get

from linkables.extract_link import ExtractLink
from linkables.link import Link
from linkables.subscriber import Subscriber


def get_rick_and_morty() -> Generator[Response, None, None]:
    response: Response = get(url='https://rickandmortyapi.com/api/')
    yield response


def main():
    headers_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.headers=}\n\n',
        subscribers=[
            Subscriber(callback=lambda value: print(value)),
        ],
    )
    json_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.json()=}\n\n',
        subscribers=Subscriber(callback=lambda value: print(value)),
    )

    rick_and_morty_extractor: ExtractLink = ExtractLink(
        processor=get_rick_and_morty,
        subscribers=[
            headers_link,
            json_link,
        ],
    )
    rick_and_morty_extractor()


if __name__ == '__main__':
    main()
