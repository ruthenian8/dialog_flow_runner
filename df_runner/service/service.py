import logging
import asyncio
import inspect
from typing import List, Optional, Callable

from df_engine.core import Actor, Context

from .utils import wrap_sync_function_in_async
from .wrapper import Wrapper
from ..types import (
    ServiceBuilder,
    StartConditionCheckerFunction,
    ComponentExecutionState,
    WrapperStage,
    WrapperFunction,
)
from ..pipeline.component import PipelineComponent
from ..conditions import always_start_condition

logger = logging.getLogger(__name__)


class Service(PipelineComponent):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        handler - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
            NB! if name is not provided, it will be generated from Actor, Function or dict.
        timeout (optionally) - the time period after that the service will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, default: always_start_condition
    """

    def __init__(
        self,
        handler: ServiceBuilder,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        asynchronous: Optional[bool] = None,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
    ):
        if isinstance(handler, dict):
            self.__init__(**handler)
        elif isinstance(handler, Service):
            self.__init__(
                **handler.get_attrs_with_updates(
                    (
                        "calculated_async_flag",
                        "path",
                    ),
                    {"requested_async_flag": "asynchronous"},
                )
            )
        elif isinstance(handler, Callable):
            self.handler = handler
            super(Service, self).__init__(
                wrappers, timeout, asynchronous, asyncio.iscoroutinefunction(handler), start_condition, name
            )
        else:
            raise Exception(f"Unknown type of service handler: {handler}")

    async def _run_handler(self, ctx: Context, actor: Actor):
        handler_params = len(inspect.signature(self.handler).parameters)
        if handler_params == 1:
            await wrap_sync_function_in_async(self.handler, ctx)
        elif handler_params == 2:
            await wrap_sync_function_in_async(self.handler, ctx, actor)
        elif handler_params == 3:
            await wrap_sync_function_in_async(self.handler, ctx, actor, self._get_runtime_info(ctx))
        else:
            raise Exception(f"Too many parameters required for service '{self.name}' handler: {handler_params}!")

    def _run_as_actor(self, ctx: Context):
        try:
            ctx = self.handler(ctx)
            self._set_state(ctx, ComponentExecutionState.FINISHED)
        except Exception as exc:
            self._set_state(ctx, ComponentExecutionState.FAILED)
            logger.error(f"Actor '{self.name}' execution failed!\n{exc}")
        return ctx

    async def _run_as_service(self, ctx: Context, actor: Actor):
        try:
            if self.start_condition(ctx, actor):
                self._set_state(ctx, ComponentExecutionState.RUNNING)
                await self._run_handler(ctx, actor)
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

        if isinstance(self.handler, Actor):
            ctx = self._run_as_actor(ctx)
        else:
            await self._run_as_service(ctx, actor)

        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.POSTPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        if isinstance(self.handler, Actor):
            return ctx

    @property
    def info_dict(self) -> dict:
        representation = super(Service, self).info_dict
        if isinstance(self.handler, Actor):
            service_representation = f"Instance of {type(self.handler).__name__}"
        elif isinstance(self.handler, Callable):
            service_representation = f"Callable '{self.handler.__name__}'"
        else:
            service_representation = "[Unknown]"
        representation.update({"handler": service_representation})
        return representation


def wrap_with(pre_func: Optional[WrapperFunction] = None, post_func: Optional[WrapperFunction] = None):
    def inner(handler: ServiceBuilder) -> Service:
        return Service(handler=handler, wrappers=[Wrapper(pre_func, post_func)])

    return inner


def with_wrappers(*wrappers: Wrapper):
    """
    A wrapper wrapping function that creates WrappedService from any service function.
    Target function will no longer be a function after wrapping; it will become a WrappedService object.
    :wrappers: - wrappers to surround the function.
    """

    def inner(handler: ServiceBuilder) -> Service:
        return Service(handler=handler, wrappers=list(wrappers))

    return inner
