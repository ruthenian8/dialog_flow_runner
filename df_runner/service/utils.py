import asyncio
from typing import Callable


async def wrap_sync_function_in_async(function: Callable, *args, **kwargs):
    if asyncio.iscoroutinefunction(function):
        await function(*args, **kwargs)
    else:
        function(*args, **kwargs)
