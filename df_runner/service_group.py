import logging
from asyncio import as_completed, TimeoutError
from typing import Optional, List, Union, Tuple, Awaitable

from df_engine.core import Actor, Context

from .service_wrapper import WrapperStage, Wrapper, execute_wrappers
from .pipe import Pipe
from .types import (
    StartConditionCheckerFunction,
    PipeExecutionState,
    StartConditionState, ServiceGroupBuilder,
)
from .service import Service
from .conditions import always_start_condition


logger = logging.getLogger(__name__)


class ServiceGroup(Pipe):
    """
    An instance that represents a service group.
    Group can be also defined in pipeline dict as a nested service list.
    Instance of this class provides possibility to define group name and wrappers.
    It accepts:
        name - custom group name (used for identification)
            NB! if name is not provided, it will be generated.
        services - a Service list in this group, may include Actor
        wrappers (optionally) - Wrapper classes array to add to all group services
    """

    def __init__(
        self,
        services: ServiceGroupBuilder,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        asynchronous: bool = True,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = "service_group",
    ):
        if isinstance(services, ServiceGroup):
            self.__init__(**vars(services))
        elif isinstance(services, dict):
            self.__init__(**services)
        elif isinstance(services, List):
            self.services = self._cast_services(services)
            asynchronous = asynchronous and all([service.asynchronous for service in self.services])
            super(ServiceGroup, self).__init__(wrappers, timeout, asynchronous, start_condition, name)
        else:
            raise Exception(f"Unknown type for ServiceGroup {services}")

    async def _run_services_group(self, ctx: Context, actor: Actor) -> Context:
        self._set_state(ctx, PipeExecutionState.RUNNING)

        service_futures = [service(ctx, actor) for service in self.services]
        for service, future in zip(self.services, as_completed(service_futures) if self.asynchronous else service_futures):
            try:
                service_result = await future
                if not service.asynchronous and isinstance(service_result, Context):
                    ctx = service_result
                elif service.asynchronous and isinstance(service_result, Awaitable):
                    await service_result
            except TimeoutError:
                logger.warning(f"{type(service).__name__} '{service.name}' timed out!")

        failed = any([service._get_state(ctx) == PipeExecutionState.FAILED for service in self.services])
        self._set_state(ctx, PipeExecutionState.FAILED if failed else PipeExecutionState.FINISHED)
        return ctx

    async def _run(
        self,
        ctx: Context,
        actor: Optional[Actor] = None,
    ) -> Optional[Context]:
        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.PREPROCESSING, self.name)

        try:
            state = self.start_condition(ctx, actor)
            if state == StartConditionState.ALLOWED:
                ctx = await self._run_services_group(ctx, actor)
            else:
                self._set_state(ctx, PipeExecutionState.FAILED)

        except Exception as e:
            self._set_state(ctx, PipeExecutionState.FAILED)
            logger.error(f"ServiceGroup '{self.name}' execution failed!\n{e}")

        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.POSTPROCESSING, self.name)
        return ctx if not self.asynchronous else None

    def get_subgroups_and_services(self, prefix: str = "", recursion_level: int = 99) -> List[Tuple[str, Service]]:
        """
        Returns a copy of created inner services flat array used during actual pipeline running.
        Breadth First Algorithm
        """
        prefix += f".{self.name}"
        services = []
        if recursion_level > 0:
            recursion_level -= 1
            services += [(prefix, service) for service in self.services]
            for service in self.services:
                if not isinstance(service, Service):
                    services += service.get_subgroups_and_services(prefix, recursion_level)
        return services

    @staticmethod
    def _cast_services(services: ServiceGroupBuilder) -> List[Union[Service, "ServiceGroup"]]:
        handled_services = []
        for service in services:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                handled_services.append(ServiceGroup(service))
            else:
                handled_services.append(Service(service))
        return handled_services
