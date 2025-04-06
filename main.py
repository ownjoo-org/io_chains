from linkables.link import Link
from linkables.subscriber import Subscriber


class PrintSubscriber(Subscriber):
    def push(self, value):
        print(f'SUBSCRIBER: {value=}', flush=True)


def main():
    linkable1 = Link(
        subscribers=PrintSubscriber(),
        processor=lambda message, *args, **kwargs: f'SUBSCRIBER PROCESSOR MESSAGE: {message=}',
    )
    linkable2 = Link(
        subscribers=PrintSubscriber(),
    )

    parent_link: Link = Link(
        in_iter=range(0, 10),
        subscribers=[linkable1, linkable2],
        processor=lambda message, *args, **kwargs: print(f'CHAIN PROCESSOR MESSAGE: {message=}'),
    )
    parent_link()


if __name__ == '__main__':
    main()
