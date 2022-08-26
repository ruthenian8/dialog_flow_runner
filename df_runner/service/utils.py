from asyncio import iscoroutinefunction
from typing import Callable

from df_engine.core import Actor

from ..types import ServiceBuilder


def name_service_handler(service_handler: ServiceBuilder) -> str:
    if isinstance(service_handler, Actor):
        return "actor"
    elif isinstance(service_handler, Callable):
        return service_handler.__name__
    else:
        return "noname"


async def wrap_sync_function_in_async(function: Callable, *args, **kwargs):
    if iscoroutinefunction(function):
        await function(*args, **kwargs)
    else:
        function(*args, **kwargs)
