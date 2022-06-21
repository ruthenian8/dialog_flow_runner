import logging
from typing import Callable, Optional, Union, Dict

from df_engine.core import Actor, Context
from pydantic import BaseModel

from df_runner import ServiceFunctionType, ServiceConditionType


logger = logging.getLogger(__name__)


def _default_start_condition(ctx: Context, actor: Actor) -> (Context, bool):
    return ctx, True


def service_successful_condition(name: str) -> ServiceConditionType:
    def internal(ctx: Context, actor: Actor) -> bool:
        return ctx.misc.get(f"{name}-success", False)
    return internal


class Service(BaseModel):
    service: Union[Actor, ServiceFunctionType]
    name: Optional[str] = None
    timeout: int = 1000
    start_condition: ServiceConditionType = _default_start_condition
    is_actor: bool = False

    def __call__(self, ctx: Context, actor: Actor, *args, **kwargs) -> Context:
        if isinstance(self.service, Actor):
            return self.service(ctx)
        else:
            context, result = self.service(ctx, actor)
            return context

    @classmethod
    def create(
        cls,
        service: Union[Actor, Dict, ServiceFunctionType],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: ServiceConditionType = _default_start_condition,
    ):
        # TODO: handling start_condition when it's str -> what does it mean??
        if isinstance(service, Actor):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, is_actor=True)
        elif isinstance(service, Callable):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, is_actor=False)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of service {service}")
