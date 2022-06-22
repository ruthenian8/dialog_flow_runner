from typing import List, Union, Dict, Optional, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel

from df_runner import Service, WrapperFunction, ServiceFunction, ServiceCondition, always_start_condition


class Wrapper(BaseModel):
    pre_func: WrapperFunction
    post_func: WrapperFunction


class WrappedService(Service):
    wrappers: List[Wrapper]

    def __call__(self, ctx: Context, actor: Actor, *args, **kwargs) -> Context:
        for wrapper in self.wrappers:
            wrapper.pre_func(ctx, actor)
        context = super(WrappedService, self).__call__(ctx, actor, *args, **kwargs)
        for wrapper in self.wrappers:
            wrapper.post_func(context, actor)
        return context

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
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, wrappers=wrappers, is_actor=True)
        elif isinstance(service, Callable):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, wrappers=wrappers, is_actor=False)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of service wrapper {service}")


def wrap(*wrappers: Wrapper):
    def inner(service: ServiceFunction) -> WrappedService:
        return WrappedService.cast(service, wrappers=list(wrappers))
    return inner
