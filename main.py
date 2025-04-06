from linkables.chain import Chain
from linkables.link import Link
from linkables.subscriber import Subscriber


class PrintSubscriber(Subscriber):
    def push(self, value):
        print(f'SUBSCRIBER: {value=}')


def main():
    linkable1 = Link(
        subscribers=PrintSubscriber(),
        processor=lambda message, *args, **kwargs: f'SUBSCRIBER PROCESSOR MESSAGE: {message=}',
    )
    linkable2 = Link(
        subscribers=PrintSubscriber(),
    )

    my_chain = Chain(
        in_iter=range(10),
        subscribers=[
            linkable1,
            linkable2,
        ],
        processor=lambda message, *args, **kwargs: print(f'CHAIN PROCESSOR MESSAGE: {message=}'),
    )
    my_chain()


if __name__ == '__main__':
    main()
