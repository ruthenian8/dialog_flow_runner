import logging
from asyncio import iscoroutinefunction
from inspect import signature
from typing import List, Optional, Callable

from df_engine.core import Actor, Context

from .utils import name_service_handler, wrap_sync_function_in_async
from .wrapper import Wrapper
from ..types import ServiceBuilder, StartConditionCheckerFunction, ComponentExecutionState, WrapperStage, WrapperFunction
from ..pipeline.component import PipelineComponent
from ..conditions import always_start_condition

logger = logging.getLogger(__name__)


class Service(PipelineComponent):
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
        service_handler: ServiceBuilder,  # NAMING: 'handler', 'operator', 'action'
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        async_flag: Optional[bool] = None,  # NAMING: 'asynchronous', user defines an __object__, this is its feature
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
    ):
        if isinstance(service_handler, dict):
            self.__init__(**service_handler)
        elif isinstance(service_handler, Service):
            self.__init__(**service_handler.cast_to_custom_dict(("calculated_async_flag",), (("requested_async_flag", "async_flag"),)))
        elif isinstance(service_handler, Callable):
            self.service_handler = service_handler
            name = name_service_handler(self.service_handler) if name is None else name
            super(Service, self).__init__(
                wrappers, timeout, async_flag, iscoroutinefunction(service_handler), start_condition, name
            )
        else:
            raise Exception(f"Unknown type of service_handler {service_handler}")

    async def _run_service_handler(self, ctx: Context, actor: Actor):  # NAMING: connected to service_handler naming
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
            self._set_state(ctx, ComponentExecutionState.FINISHED)
        except Exception as exc:
            self._set_state(ctx, ComponentExecutionState.FAILED)
            logger.error(f"Actor '{self.name}' execution failed!\n{exc}")
        return ctx

    async def _run_as_service(self, ctx: Context, actor: Actor):
        try:
            if self.start_condition(ctx, actor):
                self._set_state(ctx, ComponentExecutionState.RUNNING)
                await self._run_service_handler(ctx, actor)
                self._set_state(ctx, ComponentExecutionState.FINISHED)
            else:
                self._set_state(ctx, ComponentExecutionState.FAILED)
        except Exception as e:
            self._set_state(ctx, ComponentExecutionState.FAILED)
            logger.error(f"Service '{self.name}' execution failed!\n{e}")

    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        """
        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.PREPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        if isinstance(self.service_handler, Actor):
            ctx = self._run_as_actor(ctx)
        else:
            await self._run_as_service(ctx, actor)

        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.POSTPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        if isinstance(self.service_handler, Actor):
            return ctx

    @property
    def info_dict(self) -> dict:
        representation = super(Service, self).info_dict
        if isinstance(self.service_handler, Actor):
            service_representation = f"Instance of {type(self.service_handler).__name__}"
        elif isinstance(self.service_handler, Callable):
            service_representation = f"Callable '{self.service_handler.__name__}'"
        else:
            service_representation = "[Unknown]"
        representation.update({"service_handler": service_representation})
        return representation


def wrap_with(pre_func: Optional[WrapperFunction] = None, post_func: Optional[WrapperFunction] = None):
    def inner(service_handler: ServiceBuilder) -> Service:
        return Service(service_handler=service_handler, wrappers=[Wrapper(pre_func, post_func)])

    return inner


def with_wrappers(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """

    def inner(service_handler: ServiceBuilder) -> Service:
        return Service(service_handler=service_handler, wrappers=list(wrappers))

    return inner