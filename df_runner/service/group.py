import asyncio
import logging
from typing import Optional, List, Union, Tuple, Awaitable

from df_engine.core import Actor, Context

from .wrapper import WrapperStage, Wrapper
from ..pipeline.component import PipelineComponent
from ..types import (
    StartConditionCheckerFunction,
    ComponentExecutionState,
    ServiceGroupBuilder,
    ServiceBuilder,
)
from .service import Service
from ..conditions import always_start_condition

logger = logging.getLogger(__name__)


class ServiceGroup(PipelineComponent):
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
        timeout: Optional[int] = None,
        asynchronous: Optional[bool] = None,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
    ):
        if isinstance(services, ServiceGroup):
            self.__init__(
                **services.get_attrs_with_updates(
                    (
                        "calculated_async_flag",
                        "path",
                    ),
                    {"requested_async_flag": "asynchronous"},
                )
            )
        elif isinstance(services, dict):
            self.__init__(**services)
        elif isinstance(services, List):
            self.services = self._create_components(services)
            calc_async = all([service.asynchronous for service in self.services])
            super(ServiceGroup, self).__init__(wrappers, timeout, asynchronous, calc_async, start_condition, name)
        else:
            raise Exception(f"Unknown type for ServiceGroup {services}")

    async def _run_services_group(self, ctx: Context, actor: Actor) -> Context:
        self._set_state(ctx, ComponentExecutionState.RUNNING)

        service_futures = [service(ctx, actor) if self.asynchronous else service for service in self.services]
        for service, future in zip(
            self.services, asyncio.as_completed(service_futures) if self.asynchronous else service_futures
        ):
            try:
                service_result = await future if self.asynchronous else await future(ctx, actor)
                if not service.asynchronous and isinstance(service_result, Context):
                    ctx = service_result
                elif service.asynchronous and isinstance(service_result, Awaitable):
                    await service_result
            except asyncio.TimeoutError:
                logger.warning(f"{type(service).__name__} '{service.name}' timed out!")

        failed = any([service.get_state(ctx) == ComponentExecutionState.FAILED for service in self.services])
        self._set_state(ctx, ComponentExecutionState.FAILED if failed else ComponentExecutionState.FINISHED)
        return ctx

    async def _run(
        self,
        ctx: Context,
        actor: Optional[Actor] = None,
    ) -> Optional[Context]:
        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.PREPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        try:
            if self.start_condition(ctx, actor):
                ctx = await self._run_services_group(ctx, actor)
            else:
                self._set_state(ctx, ComponentExecutionState.FAILED)

        except Exception as e:
            self._set_state(ctx, ComponentExecutionState.FAILED)
            logger.error(f"ServiceGroup '{self.name}' execution failed!\n{e}")

        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.POSTPROCESSING, ctx, actor, self._get_runtime_info(ctx))
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

    def log_optimization_warnings(self):
        for service in self.services:
            if isinstance(service, Service):
                if (
                    service.calculated_async_flag
                    and service.requested_async_flag is not None
                    and not service.requested_async_flag
                ):
                    logger.warning(
                        f"Service '{service.name}' could be asynchronous"
                        "or should be marked as synchronous explicitly!",
                    )
                if not service.asynchronous and service.timeout is not None:
                    logger.warning(f"Timeout can not be applied for Service '{service.name}': it's not asynchronous!")
            else:
                if not service.calculated_async_flag:
                    if service.requested_async_flag is None and any(
                        [sub_service.asynchronous for sub_service in service.services]
                    ):
                        logger.warning(
                            f"ServiceGroup '{service.name}' contains both sync and async services, "
                            "it should be split or marked as synchronous explicitly!",
                        )
                    elif service.requested_async_flag:
                        logger.warning(
                            f"ServiceGroup '{service.name}' is marked asynchronous,"
                            "however contains synchronous services in it!",
                        )
                service.log_optimization_warnings()

    @property
    def info_dict(self) -> dict:
        representation = super(ServiceGroup, self).info_dict
        representation.update({"services": [service.info_dict for service in self.services]})
        return representation

    @staticmethod
    def _create_components(services: ServiceGroupBuilder) -> List[Union[ServiceBuilder, ServiceGroupBuilder]]:
        handled_services = []
        for service in services:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                handled_services.append(ServiceGroup(service))
            else:
                handled_services.append(Service(service))
        return handled_services
