from linkable import Linkable
from subscriber import Subscriber


class PrintSubscriber(Subscriber):
    def push(self, value):
        print(f'SUBSCRIBER: {value=}')


def main():
    subscriber1 = PrintSubscriber()
    subscriber2 = PrintSubscriber()
    linkable1 = Linkable(subscribers=subscriber1)
    linkable2 = Linkable(subscribers=subscriber2)
    my_chain = Linkable(
        in_iter=range(10),
        processor=lambda message, *args, **kwargs: message,
        subscribers=[
            linkable1,
            linkable2,
        ],
    )
    my_chain()


if __name__ == '__main__':
    main()
