from linkable import Linkable
from subscriber import Subscriber


class PrintSubscriber(Subscriber):
    def push(self, value):
        print(f'SUBSCRIBER: {value=}')


def main():
    linkable1 = Linkable(
        subscribers=PrintSubscriber(),
        processor=lambda message, *args, **kwargs: f'SUBSCRIBER MESSAGE: {message=}',
    )
    linkable2 = Linkable(
        subscribers=PrintSubscriber(),
    )
    my_chain = Linkable(
        in_iter=range(10),
        subscribers=[
            linkable1,
            linkable2,
        ],
        processor=lambda message, *args, **kwargs: print(f'PROCESSOR MESSAGE: {message=}'),
    )
    my_chain()


if __name__ == '__main__':
    main()
