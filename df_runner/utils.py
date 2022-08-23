from asyncio import get_running_loop, run
from typing import Awaitable, Any


def run_in_current_or_new_loop(future: Awaitable) -> Any:
    try:
        loop = get_running_loop()
        return loop.run_until_complete(future)
    except RuntimeError:
        return run(future)
