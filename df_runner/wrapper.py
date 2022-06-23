from typing import List, Union, Dict, Optional, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel

from df_runner import Service, WrapperFunction, ServiceFunction, ServiceCondition
from df_runner.conditions import always_start_condition


class Wrapper(BaseModel):
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after service.
    Wrappers should execute supportive tasks (like time or resources measurement).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    """

    pre_func: WrapperFunction
    post_func: WrapperFunction


class WrappedService(Service):
    """
    Class that represents a service wrapped by some wrappers.
    They are executed right before and after the service itself.
    """

    wrappers: Optional[List[Wrapper]] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.wrappers = [] if self.wrappers is None else self.wrappers

    def __call__(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Context:
        for wrapper in self.wrappers:
            wrapper.pre_func(ctx, actor)
        ctx = super(WrappedService, self).__call__(ctx, actor, *args, **kwargs)
        for wrapper in self.wrappers:
            wrapper.post_func(ctx, actor)
        return ctx

    @classmethod
    def cast(
        cls,
        service: Union[Actor, Dict, ServiceFunction, Service],
        naming: Optional[Dict[str, int]] = None,
        name: Optional[str] = None,
        groups: Optional[List[str]] = None,
        wrappers: Optional[List[Wrapper]] = None,
        **kwargs
    ):
        wrappers = [] if wrappers is None else wrappers
        super().cast(service, naming, name, groups, wrappers=wrappers, **kwargs)


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """
    def inner(service: ServiceFunction) -> WrappedService:
        return WrappedService(service=service, wrappers=list(wrappers))
    return inner
