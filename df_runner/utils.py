from asyncio import get_running_loop, run
from typing import Awaitable, Any, Coroutine, Union


def run_in_current_or_new_loop(future: Union[Awaitable, Coroutine]) -> Any:
    try:
        loop = get_running_loop()
        if loop.is_running():
            return loop.create_task(future)
        else:
            return loop.run_until_complete(future)
    except RuntimeError:
        return run(future)
