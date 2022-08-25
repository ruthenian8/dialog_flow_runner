import logging
from asyncio import iscoroutinefunction
from inspect import signature
from typing import List, Optional, Callable

from df_engine.core import Actor, Context

from .service_utils import name_service_handler, wrap_sync_function_in_async
from .service_wrapper import Wrapper
from .types import ServiceBuilder, StartConditionCheckerFunction, PipeExecutionState, WrapperStage, WrapperFunction
from .pipe import Pipe
from .conditions import always_start_condition


logger = logging.getLogger(__name__)


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
        timeout: Optional[int] = None,
        asynchronous: Optional[bool] = None,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
    ):
        if isinstance(service_handler, dict):
            self.__init__(**service_handler)
        elif isinstance(service_handler, Service):
            service_dict = vars(service_handler)
            service_dict["asynchronous"] = service_dict.pop("_user_async")
            service_dict.pop("_calc_async")
            self.__init__(**service_dict)
        elif isinstance(service_handler, Callable):
            self.service_handler = service_handler
            name = name_service_handler(self.service_handler) if name is None else name
            super(Service, self).__init__(
                wrappers, timeout, asynchronous, iscoroutinefunction(service_handler), start_condition, name
            )
        else:
            raise Exception(f"Unknown type of service_handler {service_handler}")

    async def _run_service_handler(self, ctx: Context, actor: Actor):
        handler_params = len(signature(self.service_handler).parameters)
        if handler_params == 1:
            await wrap_sync_function_in_async(self.service_handler, ctx)
        elif handler_params == 2:
            await wrap_sync_function_in_async(self.service_handler, ctx, actor)
        elif handler_params == 3:
            await wrap_sync_function_in_async(self.service_handler, ctx, actor, self._get_runtime_info(ctx))
        else:
            raise Exception(f"Too many parameters required for service '{self.name}' handler: {handler_params}!")

    def _run_as_actor(self, ctx: Context):
        try:
            ctx = self.service_handler(ctx)
            self._set_state(ctx, PipeExecutionState.FINISHED)
        except Exception as exc:
            self._set_state(ctx, PipeExecutionState.FAILED)
            logger.error(f"Actor '{self.name}' execution failed!\n{exc}")
        return ctx

    async def _run_as_service(self, ctx: Context, actor: Actor):
        try:
            if self.start_condition(ctx, actor):
                self._set_state(ctx, PipeExecutionState.RUNNING)
                await self._run_service_handler(ctx, actor)
                self._set_state(ctx, PipeExecutionState.FINISHED)
            else:
                self._set_state(ctx, PipeExecutionState.FAILED)
        except Exception as e:
            self._set_state(ctx, PipeExecutionState.FAILED)
            logger.error(f"Service '{self.name}' execution failed!\n{e}")

    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        """
        for wrapper in self.wrappers:
            wrapper.run_wrapper_function(WrapperStage.PREPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        if isinstance(self.service_handler, Actor):
            ctx = self._run_as_actor(ctx)
        else:
            await self._run_as_service(ctx, actor)

        for wrapper in self.wrappers:
            wrapper.run_wrapper_function(WrapperStage.POSTPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        if isinstance(self.service_handler, Actor):
            return ctx

    def to_string(self, show_wrappers: bool = False, offset: str = "") -> str:
        representation = super(Service, self).to_string(show_wrappers, offset)
        if isinstance(self.service_handler, Actor):
            service_representation = str(self.service_handler)
        elif isinstance(self.service_handler, Callable):
            service_representation = f"Callable '{self.service_handler.__name__}'"
        else:
            service_representation = "[Unknown]"
        representation += f"{offset}\tservice_handler: {service_representation}"
        return representation


def add_wrapper(pre_func: Optional[WrapperFunction] = None, post_func: Optional[WrapperFunction] = None):
    def inner(service_handler: ServiceBuilder) -> Service:
        return Service(service_handler=service_handler, wrappers=[Wrapper(pre_func, post_func)])

    return inner


def wrap(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """

    def inner(service_handler: ServiceBuilder) -> Service:
        return Service(service_handler=service_handler, wrappers=list(wrappers))

    return inner
