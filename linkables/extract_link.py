from linkables.link import Link


class ExtractLink(Link):
    def _publish(self) -> None:
        if self._subscribers:
            for each in self._processor():
                for subscriber in self._subscribers:
                    subscriber.push(each)

    def __call__(self) -> None:
        if self._subscribers:
            self._publish()
        else:
            return self._processor()
