from asyncio import get_running_loop, run
from logging import getLogger
from typing import Any, AsyncIterable, Generator, Callable

import nest_asyncio

logger = getLogger(__name__)


def iter_over_async(a_iter: AsyncIterable) -> Generator[Any, Any, Any]:
    ait = a_iter.__aiter__()

    async def get_next():
        try:
            obj = await ait.__anext__()
            return False, obj
        except StopAsyncIteration:
            return True, None

    runner: Callable = run
    try:
        loop = get_running_loop()
        nest_asyncio.apply(loop=loop)
        runner = loop.run_until_complete
    except Exception as e:
        logger.warning(f'\n\n{__name__=}: {e}\n\n')

    is_done: bool = False
    while not is_done:
        is_done, next_obj = runner(get_next())
        if not is_done:
            yield next_obj
