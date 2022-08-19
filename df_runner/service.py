import logging
from asyncio import iscoroutinefunction
from typing import Optional, Union, Dict, Callable, Literal, Any, Set

from df_engine.core import Actor, Context

from .named import Named
from .wrapper import Wrapper, WrapperType
from .types import ACTOR, ServiceFunction, ServiceCondition, ServiceState, ConditionState
from .runnable import Runnable
from .conditions import always_start_condition


logger = logging.getLogger(__name__)


class Service(Runnable, Named):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        service - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
            NB! if name is not provided, it will be generated from Actor, Function or dict.
        timeout (optionally) - the time period after that the service will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, default: always_start_condition
    """

    service: Union[Literal[ACTOR], ServiceFunction]
    timeout: int = -1
    start_condition: ServiceCondition = always_start_condition

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        """
        if self.service == ACTOR:
            ctx = actor(ctx)
            self._framework_states_runner(ctx, ServiceState.FINISHED)
            return ctx

        self._framework_states_meta(ctx, dict())
        self._export_data(ctx, actor, WrapperType.PREPROCESSING)

        result = None
        try:
            state = self.start_condition(ctx, actor)
            if state == ConditionState.ALLOWED:
                if iscoroutinefunction(self.service):
                    self._framework_states_runner(ctx, ServiceState.RUNNING)
                    result = await self.service(ctx, actor)
                    self._framework_states_runner(ctx, ServiceState.FINISHED)
                else:
                    result = self.service(ctx, actor)
                    self._framework_states_runner(ctx, ServiceState.FINISHED)
            elif state == ConditionState.PENDING:
                self._framework_states_runner(ctx, ServiceState.PENDING)
            else:
                self._framework_states_runner(ctx, ServiceState.FAILED)

        except Exception as e:
            self._framework_states_runner(ctx, ServiceState.FAILED)
            logger.error(f"Service {self.name} execution failed for unknown reason!\n{e}")

        self._export_data(ctx, actor, WrapperType.POSTPROCESSING, result)

    @staticmethod
    def _get_name(
        service: Union[Literal[ACTOR], Dict, ServiceFunction],
        forbidden_names: Optional[Set[str]] = None,
        name_rule: Optional[Callable[[Any], str]] = None,
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None,
    ) -> str:
        def default_name_rule(this: Union[Actor, Dict, ServiceFunction]) -> str:
            if isinstance(this, str) and this == ACTOR:
                return "actor"
            elif isinstance(this, Service):
                return "serv"
            elif isinstance(this, Callable):
                return f"func_{this.__name__}"
            else:
                return "unknown"

        forbidden_names = forbidden_names if forbidden_names is not None else {"actor_", "func_", "obj_", "group_"}
        name_rule = name_rule if name_rule is not None else default_name_rule
        return super(Service, Service)._get_name(service, name_rule, forbidden_names, naming, given_name)

    @classmethod
    def cast(
        cls,
        service: Union[Literal[ACTOR], Dict, ServiceFunction, "Service"],
        naming: Optional[Dict[str, int]] = None,
        **kwargs,
    ) -> "Service":
        """
        Method for service creation from actor, function or dict.
        No other sources are accepted (yet).
        """
        naming = {} if naming is None else naming
        if isinstance(service, Service):
            service.name = cls._get_name(service.service, naming=naming, given_name=service.name)
            service.asynchronous = iscoroutinefunction(service.service)
            return service
        elif service == ACTOR or isinstance(service, Callable):
            service = cls(service=service, name=cls._get_name(service, naming=naming), **kwargs)
            service.asynchronous = iscoroutinefunction(service.service)
            return service
        else:
            raise Exception(f"Unknown type of service {service}")


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """

    def inner(service: ServiceFunction) -> Service:
        return Service(service=service, wrappers=list(wrappers))

    return inner
