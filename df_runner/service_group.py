import logging
from asyncio import wait_for, create_task, as_completed, TimeoutError as AsyncTimeoutError
from typing import Optional, List, Union

from df_engine.core import Actor, Context

from .service_wrapper import WrapperStage, Wrapper, execute_wrappers
from .state_tracker import StateTracker
from .types import (
    Handler,
    ServiceCondition,
    ServiceState,
    ConditionState,
)
from .service import Service
from .conditions import always_start_condition


_ServiceCallable = Union[Service, Handler, Actor]

logger = logging.getLogger(__name__)


class ServiceGroup(StateTracker):
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
        services: Optional[Union[List[Union[_ServiceCallable, List[_ServiceCallable], "ServiceGroup"]], "ServiceGroup"]] = None,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        asynchronous: bool = True,
        start_condition: ServiceCondition = always_start_condition,
        name: Optional[str] = "service_group",
    ):
        services = [] if services is None else services
        if isinstance(services, ServiceGroup):
            self.__init__(**vars(services))
        elif isinstance(services, dict):
            self.__init__(**services)
        elif isinstance(services, List):
            self.services = self._cast_services(services)
            services_asynchronous = all([service.asynchronous for service in self.services])
            self.wrappers = [] if wrappers is None else wrappers
            self.timeout = timeout
            self.asynchronous = asynchronous and services_asynchronous
            self.start_condition = start_condition
            self.name = name
        else:
            raise Exception(f"Unknown type for ServiceGroup {services}")

    async def _run_service_group(
        self,
        ctx: Context,
        actor: Optional[Actor] = None,
        *args,
        **kwargs,
    ) -> Optional[Context]:
        try:
            state = self.start_condition(ctx, actor)
            if state == ConditionState.ALLOWED:

                if self.asynchronous:
                    self._set_state(ctx, ServiceState.RUNNING)

                    running = dict()
                    for service in self.services:
                        if service._get_state(ctx, default=ServiceState.NOT_RUN) is ServiceState.NOT_RUN:
                            service_result = create_task(service(ctx, actor), name=service.name)
                            timeout = service.timeout if service.timeout > -1 else None
                            running.update({service.name: wait_for(service_result, timeout=timeout)})
                        self._set_state(ctx, None)

                    for name, future in zip(running.keys(), as_completed(running.values())):
                        try:
                            await future
                        except AsyncTimeoutError:
                            logger.warning(f"Service {name} timed out!")

                    failed = False
                    for service in self.services:
                        if service._get_state(ctx) == ServiceState.PENDING:
                            service._set_state(ctx, ServiceState.FAILED)
                            failed = True
                        self._set_state(ctx, None)
                    self._set_state(ctx, ServiceState.FAILED if failed else ServiceState.FINISHED)

                else:
                    for service in self.services:
                        service_result = None
                        if service.asynchronous:
                            timeout = service.timeout if service.timeout > -1 else None
                            task = create_task(service(ctx, actor, *args, **kwargs), name=service.name)
                            future = wait_for(task, timeout=timeout)
                            try:
                                await future
                            except AsyncTimeoutError:
                                logger.warning(f"{type(service).__name__} {service.name} timed out!")
                        else:
                            service_result = await service(ctx, actor)
                        if isinstance(service_result, Context):
                            ctx = service_result

                    self._set_state(ctx, ServiceState.FINISHED)

            elif state == ConditionState.PENDING:
                self._set_state(ctx, ServiceState.PENDING)
            else:
                self._set_state(ctx, ServiceState.FAILED)

        except Exception as e:
            self._set_state(ctx, ServiceState.FAILED)
            logger.error(f"ServiceGroup {self.name} execution failed for unknown reason!\n{e}")

        return ctx

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        self._set_state(ctx, dict())
        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.PREPROCESSING, self.name)

        timeout = self.timeout if self.timeout > -1 else None
        if self.asynchronous:
            task = create_task(self._run_service_group(ctx, actor, *args, **kwargs), name=self.name)
            future = wait_for(task, timeout=timeout)
            try:
                await future
            except AsyncTimeoutError:
                logger.warning(f"ServiceGroup {self.name} timed out!")
        else:
            if timeout is not None:
                logger.warning(f"Timeout can not be applied for group {self.name}: it is not asynchronous !")
            ctx = await self._run_service_group(ctx, actor, *args, **kwargs)

        execute_wrappers(ctx, actor, self.wrappers, WrapperStage.POSTPROCESSING, self.name)
        return ctx

    @staticmethod
    def _cast_services(
        services: List[Union[_ServiceCallable, List[_ServiceCallable], "ServiceGroup"]],
    ) -> List[Union[Service, "ServiceGroup"]]:
        handled_services = []
        for service in services:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                handled_services.append(ServiceGroup(service))
            else:
                handled_services.append(Service(service))
        return handled_services
