from typing import Optional, Union, Dict, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel, Extra

from df_runner import ServiceFunction, ServiceCondition
from df_runner.conditions import always_start_condition


class Service(BaseModel):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        service - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
        timeout (optionally) - the time period the service is allowed to run, it will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, service will run only if this returned True, default: always_start_condition
    """

    service: Union[Actor, ServiceFunction]
    name: Optional[str] = None
    timeout: int = 1000
    start_condition: ServiceCondition = always_start_condition

    class Config:
        extra = Extra.allow

    def __call__(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Context:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        If execution fails the error is caught here and TODO: set special value in context for other services start_conditions.
        """
        try:
            if isinstance(self.service, Actor):
                return self.service(ctx)
            elif actor is None:
                raise Exception(f"For service {self.name} no actor has been provided!")
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
        """
        Method for service creation from actor, function or dict.
        No other sources are accepted (yet).
        """
        if isinstance(service, Actor) or isinstance(service, Callable):
            return cls(service=service, name=name, timeout=timeout, start_condition=start_condition)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of service {service}")
