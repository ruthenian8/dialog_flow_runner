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
        timeout: int = 1000,
        start_condition: ServiceCondition = always_start_condition,
        wrappers: Optional[List[Wrapper]] = None
    ):
        wrappers = [] if wrappers is None else wrappers
        if isinstance(service, Service):
            service.name = cls._get_name(service, naming, service.name)
            return service
        elif isinstance(service, Actor) or isinstance(service, Callable):
            return cls(
                service=service,
                name=cls._get_name(service, naming, name),
                timeout=timeout,
                start_condition=start_condition,
                wrappers=wrappers
            )
        else:
            raise Exception(f"Unknown type of wrapped service {service}")


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    NB! Generated Service name will imply it was an object as it, actually, really was.
    :wrappers: - wrappers to surround the function.
    """
    def inner(service: ServiceFunction) -> WrappedService:
        return WrappedService(service=service, wrappers=list(wrappers))
    return inner
