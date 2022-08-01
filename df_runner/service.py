import logging
from asyncio import iscoroutinefunction
from typing import Optional, Union, Dict, Callable, List, Literal, Any

from df_engine.core import Actor, Context
from pydantic import BaseModel, Extra

from .wrapper import Wrapper, WrapperType
from .types import ACTOR, ServiceFunction, ServiceCondition, FrameworkKeys, ServiceState, ConditionState
from .runnable import Runnable
from .conditions import always_start_condition


logger = logging.getLogger(__name__)


class Service(BaseModel, Runnable):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        service - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
            NB! if name is not provided, it will be generated from Actor, Function or dict.
        timeout (optionally) - the time period the service is allowed to run, it will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, service will run only if this returned True, default: always_start_condition
    """

    service: Union[Literal[ACTOR], ServiceFunction]
    name: Optional[str] = None
    timeout: int = -1
    start_condition: ServiceCondition = always_start_condition
    wrappers: Optional[List[Wrapper]] = None

    class Config:
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.wrappers = [] if self.wrappers is None else self.wrappers
        self.asynchronous = False

    async def __call__(self, ctx: Context, callback: Callable[[str, FrameworkKeys, Any], None], actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        """
        if self.service == ACTOR:
            ctx = actor(ctx)
            ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.FINISHED
            return ctx

        ctx.framework_states[FrameworkKeys.SERVICES_META][self.name] = dict()
        for wrapper in self.wrappers:
            self._export_wrapper_data(wrapper.pre_func(ctx, actor), ctx, wrapper.name, WrapperType.PREPROCESSING, callback)

        result = None
        try:
            state = self.start_condition(ctx, actor)
            if state == ConditionState.ALLOWED:
                if iscoroutinefunction(self.service):
                    ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.RUNNING
                    result = await self.service(ctx, actor)
                else:
                    result = self.service(ctx, actor)
                    ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.FINISHED
            elif state == ConditionState.PENDING:
                ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.PENDING
            else:
                ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.FAILED

        except Exception as e:
            ctx.framework_states[FrameworkKeys.RUNNER][self.name] = ServiceState.FAILED
            logger.error(f"Service {self.name} execution failed for unknown reason!\n{e}")

        self._export_data(result, ctx, callback)

        for wrapper in self.wrappers:
            self._export_wrapper_data(wrapper.post_func(ctx, actor), ctx, wrapper.name, WrapperType.POSTPROCESSING, callback)

    @staticmethod
    def _get_name(
        service: Union[Actor, Dict, ServiceFunction],
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None
    ) -> str:
        """
        Method for name generation.
        Name is generated using following convention:
            actor: 'actor_[NUMBER]'
            function: 'func_[REPR]_[NUMBER]'
            service object: 'obj_[NUMBER]'
        If user provided name uses same syntax it will be changed to auto-generated.
        """
        if given_name is not None and not (given_name.startswith('actor_') or given_name.startswith('func_') or given_name.startswith('obj_') or given_name.startswith('group_')):
            if naming is not None:
                if given_name in naming:
                    raise Exception(f"User defined service name collision: {given_name}")
                else:
                    naming[given_name] = True
            return given_name
        elif given_name is not None:
            logger.warning(f"User defined name for service '{given_name}' violates naming convention, the service will be renamed")

        if isinstance(service, Actor):
            name = 'actor'
        elif isinstance(service, Service):
            name = 'serv'
        elif isinstance(service, Callable):
            name = f'func_{service.__name__}'
        else:
            name = 'unknown'

        if naming is not None:
            number = naming.get(name, 0)
            naming[name] = number + 1
            return f'{name}_{number}'
        else:
            return name

    @classmethod
    def cast(
        cls,
        service: Union[Literal[ACTOR], Dict, ServiceFunction, 'Service'],
        naming: Optional[Dict[str, int]] = None,
        **kwargs
    ) -> 'Service':
        """
        Method for service creation from actor, function or dict.
        No other sources are accepted (yet).
        """
        naming = {} if naming is None else naming
        if isinstance(service, Service):
            service.name = cls._get_name(service.service, naming, service.name)
            service.asynchronous = iscoroutinefunction(service.service)
            return service
        elif service == ACTOR or isinstance(service, Callable):
            service = cls(
                service=service,
                name=cls._get_name(service, naming),
                **kwargs
            )
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
