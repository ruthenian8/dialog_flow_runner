import asyncio
from typing import Callable, Any, Optional, List

from df_runner import Wrapper, StartConditionCheckerFunction


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


def collect_defined_constructor_parameters_to_dict(
    wrappers: Optional[List[Wrapper]] = None,
    timeout: Optional[int] = None,
    asynchronous: Optional[bool] = None,
    start_condition: Optional[StartConditionCheckerFunction] = None,
    name: Optional[str] = None,
):
    """
    Function, that creates dict from non-None constructor parameters of pipeline component.
    It is used in overriding component parameters,
        when service handler or service group service is instance of Service or ServiceGroup (or dict).
    It accepts same named parameters as component constructor.
    Returns dict, containing key-value pairs of these parameters, that are not None.
    """
    return dict([(key, value) for key, value in {
        "wrappers": wrappers,
        "timeout": timeout,
        "asynchronous": asynchronous,
        "start_condition": start_condition,
        "name": name,
    }.items() if value is not None])
