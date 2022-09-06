import asyncio
import logging
from typing import Optional, List, Union, Awaitable

from df_engine.core import Actor, Context

from .wrapper import WrapperStage, Wrapper
from ..pipeline.component import PipelineComponent
from ..types import (
    StartConditionCheckerFunction,
    ComponentExecutionState,
    ServiceGroupBuilder,
    GlobalWrapperType,
    WrapperConditionFunction,
    WrapperFunction,
)
from .service import Service
from ..conditions import always_start_condition

logger = logging.getLogger(__name__)


class ServiceGroup(PipelineComponent):
    """
    This class represents a service group.
    Service group can be included into pipeline as object or a pipeline component list.
    Service group can be synchronous or asynchronous.
    Components in synchronous groups are executed consequently (no matter is they are synchronous or asynchronous).
    Components in asynchronous groups are executed simultaneously.
    Group can be asynchronous only if all components in it are asynchronous.
    Group containing actor can be synchronous only.
    It accepts constructor parameters:
        `services` (required) - a ServiceGroupBuilder object, that will be added to the group
        `wrappers` - list of Wrappers to add to the group
        `timeout` - timeout to add to the group
        `asynchronous` - requested asynchronous property
        `start_condition` - StartConditionCheckerFunction that is invoked before each group execution;
            group is executed only if it returns True
        `name` - requested group name
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
        """
        Method for running this service group.
        It doesn't include wrappers execution, start condition checking or error handling - pure execution only.
        Executes components inside the group based on its `asynchronous` property.
        Collects information about their execution state - group is finished successfully
            only if all components in it finished successfully.
        :ctx: - current dialog context.
        :actor: - actor, associated with the pipeline.
        Returns current dialog context.
        """
        self._set_state(ctx, ComponentExecutionState.RUNNING)

        if self.asynchronous:
            service_futures = [service(ctx, actor) for service in self.services]
            for service, future in zip(self.services, asyncio.as_completed(service_futures)):
                try:
                    service_result = await future
                    if service.asynchronous and isinstance(service_result, Awaitable):
                        await service_result
                except asyncio.TimeoutError:
                    logger.warning(f"{type(service).__name__} '{service.name}' timed out!")

        else:
            for service in self.services:
                service_result = await service(ctx, actor)
                if not service.asynchronous and isinstance(service_result, Context):
                    ctx = service_result
                elif service.asynchronous and isinstance(service_result, Awaitable):
                    await service_result

        failed = any([service.get_state(ctx) == ComponentExecutionState.FAILED for service in self.services])
        self._set_state(ctx, ComponentExecutionState.FAILED if failed else ComponentExecutionState.FINISHED)
        return ctx

    async def _run(
        self,
        ctx: Context,
        actor: Actor = None,
    ) -> Optional[Context]:
        """
        Method for handling this group execution.
        Executes before and after execution wrappers, checks start condition and catches runtime exceptions.
        :ctx: - current dialog context.
        :actor: - actor, associated with the pipeline.
        Returns current dialog context if synchronous, else None.
        """
        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.PREPROCESSING, ctx, actor, self._get_runtime_info(ctx))

        try:
            if self.start_condition(ctx, actor):
                ctx = await self._run_services_group(ctx, actor)
            else:
                self._set_state(ctx, ComponentExecutionState.NOT_RUN)

        except Exception as e:
            self._set_state(ctx, ComponentExecutionState.FAILED)
            logger.error(f"ServiceGroup '{self.name}' execution failed!\n{e}")

        for wrapper in self.wrappers:
            wrapper.run_stage(WrapperStage.POSTPROCESSING, ctx, actor, self._get_runtime_info(ctx))
        return ctx if not self.asynchronous else None

    def log_optimization_warnings(self):
        """
        Method for logging service group optimization warnings for all this groups inner components
            (NOT this group itself!).
        Warnings are basically messages,
            that indicate service group inefficiency or explicitly defined parameters mismatch.
        These are cases for warnings issuing:
            1. Service can be asynchronous, however is marked synchronous explicitly
            2. Service is not asynchronous, however has a timeout defined
            3. Group is not marked synchronous explicitly and contains both synchronous and asynchronous components
        Returns None.
        """
        for service in self.services:
            if isinstance(service, Service):
                if (
                    service.calculated_async_flag
                    and service.requested_async_flag is not None
                    and not service.requested_async_flag
                ):
                    logger.warning(f"Service '{service.name}' could be asynchronous!")
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
                service.log_optimization_warnings()

    def add_wrapper(
        self,
        global_wrapper_type: GlobalWrapperType,
        wrapper: WrapperFunction,
        condition: WrapperConditionFunction = lambda _: True,
    ):
        """
        Method for adding a global wrapper to this group.
        Adds wrapper to itself and propagates it to all inner components.
        Uses a special condition function to determine whether to add wrapper to any particular inner component or not.
        Condition checks components path to be in whitelist (if defined) and not to be in blacklist (if defined).
        :global_wrapper_type: - a type of wrapper to add.
        :wrapper: - a WrapperFunction to add as a wrapper.
        :condition: - a condition function.
        Returns None.
        """
        super().add_wrapper(global_wrapper_type, wrapper)
        for service in self.services:
            if not condition(service.path):
                continue
            if isinstance(service, Service):
                service.add_wrapper(global_wrapper_type, wrapper)
            else:
                service.add_wrapper(global_wrapper_type, wrapper, condition)

    @property
    def info_dict(self) -> dict:
        """
        See `Component.info_dict` property.
        Adds `services` key to base info dictionary.
        """
        representation = super(ServiceGroup, self).info_dict
        representation.update({"services": [service.info_dict for service in self.services]})
        return representation

    @staticmethod
    def _create_components(services: ServiceGroupBuilder) -> List[Union[Service, "ServiceGroup"]]:
        """
        Utility method, used to create inner components, judging by their nature.
        Services are created from services and dictionaries.
        ServiceGroups are created from service groups and lists.
        :services: - ServiceGroupBuilder object (a ServiceGroup instance or a list).
        Returns list of services and service groups.
        """
        handled_services = []
        for service in services:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                handled_services.append(ServiceGroup(service))
            else:
                handled_services.append(Service(service))
        return handled_services
