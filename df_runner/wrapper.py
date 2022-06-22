from typing import List, Union, Dict, Optional, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel

from df_runner import Service, WrapperFunction, ServiceFunction, ServiceCondition, always_start_condition


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

    wrappers: List[Wrapper]

    def __call__(self, ctx: Context, actor: Actor, *args, **kwargs) -> Context:
        for wrapper in self.wrappers:
            wrapper.pre_func(ctx, actor)
        ctx = super(WrappedService, self).__call__(ctx, actor, *args, **kwargs)
        for wrapper in self.wrappers:
            wrapper.post_func(ctx, actor)
        return ctx

    @classmethod
    def cast(
        cls,
        service: Union[Actor, Dict, ServiceFunction],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: ServiceCondition = always_start_condition,
        wrappers: Optional[List[Wrapper]] = None
    ):
        wrappers = [] if wrappers is None else wrappers
        if isinstance(service, Actor):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, wrappers=wrappers)
        elif isinstance(service, Callable):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, wrappers=wrappers)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of wrapped service {service}")


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    :wrappers: - wrappers to surround the function.
    """
    def inner(service: ServiceFunction) -> WrappedService:
        return WrappedService.cast(service, wrappers=list(wrappers))
    return inner
