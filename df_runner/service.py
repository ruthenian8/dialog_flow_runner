import logging
from asyncio import iscoroutinefunction
from typing import List, Optional, Callable

from df_engine.core import Actor, Context

from .service_wrapper import Wrapper, WrapperStage, execute_wrappers
from .types import ServiceBuilder, StartConditionCheckerFunction, ServiceExecutionState, StartConditionState
from .pipe import Pipe
from .conditions import always_start_condition


logger = logging.getLogger(__name__)


def name_service_handler(service_handler: ServiceBuilder) -> str:
    if isinstance(service_handler, Actor):
        return "actor"
    elif isinstance(service_handler, Service):
        service: Service = service_handler
        return service.name if service.name else name_service_handler(service.service_handler)
    elif isinstance(service_handler, Callable):
        return service_handler.__name__
    else:
        return "noname"


class Service(Pipe):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        service_handler - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
            NB! if name is not provided, it will be generated from Actor, Function or dict.
        timeout (optionally) - the time period after that the service will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, default: always_start_condition
    """

    def __init__(
        self,
        service_handler: ServiceBuilder,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        asynchronous: bool = True,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
    ):
        if isinstance(service_handler, dict):
            self.__init__(**service_handler)
        elif isinstance(service_handler, Service):
            self.__init__(**vars(service_handler))
        elif isinstance(service_handler, Callable):
            self.service_handler = service_handler
            name = name_service_handler(self.service_handler) if name is None else name
            asynchronous = asynchronous and iscoroutinefunction(service_handler)
            super(Service, self).__init__(wrappers, timeout, asynchronous, start_condition, name)
        else:
            raise Exception(f"Unknown type of service_handler {service_handler}")

    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        """
        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.PREPROCESSING, self.name)

        if isinstance(self.service_handler, Actor):
            try:
                ctx = self.service_handler(ctx)
                self._set_state(ctx, ServiceExecutionState.FINISHED)
            except Exception as exc:
                self._set_state(ctx, ServiceExecutionState.FAILED)
                logger.error(f"Actor '{self.name}' execution failed!\n{exc}")

            execute_wrappers(ctx, actor, self.wrappers, WrapperStage.POSTPROCESSING, self.name)
            return ctx

        try:
            state = self.start_condition(ctx, actor)
            if state == StartConditionState.ALLOWED:
                if iscoroutinefunction(self.service_handler):
                    self._set_state(ctx, ServiceExecutionState.RUNNING)
                    await self.service_handler(ctx, actor)
                    self._set_state(ctx, ServiceExecutionState.FINISHED)
                else:
                    self.service_handler(ctx, actor)
                    self._set_state(ctx, ServiceExecutionState.FINISHED)
            elif state == StartConditionState.PENDING:
                self._set_state(ctx, ServiceExecutionState.PENDING)
            else:
                self._set_state(ctx, ServiceExecutionState.FAILED)

        except Exception as e:
            self._set_state(ctx, ServiceExecutionState.FAILED)
            logger.error(f"Service '{self.name}' execution failed!\n{e}")

        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.POSTPROCESSING, self.name)


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """

    def inner(service_handler: ServiceBuilder) -> Service:
        return Service(service_handler=service_handler, wrappers=list(wrappers))

    return inner
