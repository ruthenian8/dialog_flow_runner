import logging
from typing import Optional, Union, Dict, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel

from df_runner import ServiceFunction, ServiceCondition, always_start_condition

logger = logging.getLogger(__name__)


class Service(BaseModel):
    service: Union[Actor, ServiceFunction]
    name: Optional[str] = None
    timeout: int = 1000
    start_condition: ServiceCondition = always_start_condition
    is_actor: bool = False

    def __call__(self, ctx: Context, actor: Actor, *args, **kwargs) -> Context:
        try:
            if isinstance(self.service, Actor):
                return self.service(ctx)
            else:
                return self.service(ctx, actor)
        except Exception as e:
            print(str(e))
            return ctx

    @classmethod
    def cast(
        cls,
        service: Union[Actor, Dict, ServiceFunction],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: ServiceCondition = always_start_condition
    ):
        if isinstance(service, Actor):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, is_actor=True)
        elif isinstance(service, Callable):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition, is_actor=False)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of service {service}")
