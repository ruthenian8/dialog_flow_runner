import asyncio
from typing import Callable, Any


async def wrap_sync_function_in_async(function: Callable, *args, **kwargs) -> Any:
    """
    Utility function, that wraps both functions and coroutines in coroutines.
    Invokes function if it is just a callable and awaits - if this is a coroutine.
    :function: - callable to wrap.
    :*args: - function args.
    :**kwargs: - function kwargs.
    Returns what function returns.
    """
    if asyncio.iscoroutinefunction(function):
        return await function(*args, **kwargs)
    else:
        return function(*args, **kwargs)
