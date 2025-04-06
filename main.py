from typing import Generator

from requests import Response, get

from linkables.extract_link import ExtractLink
from linkables.link import Link
from linkables.subscriber import Subscriber


class PrintSubscriber(Subscriber):
    def push(self, value):
        print(value, flush=True)


def get_rick_and_morty() -> Generator[Response, None, None]:
    response: Response = get(url='https://rickandmortyapi.com/api/')
    yield response


def main():
    headers_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.headers=}\n\n',
        subscribers=PrintSubscriber(),
    )
    json_link = Link(
        processor=lambda resp, *args, **kwargs: f'\n\n{resp.json()=}\n\n',
        subscribers=PrintSubscriber(),
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
