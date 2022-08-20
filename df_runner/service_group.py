import logging
from asyncio import wait_for, create_task, as_completed, TimeoutError as AsyncTimeoutError
from typing import Optional, List, Union, Dict, Literal, Callable, Any, Set

from df_engine.core import Actor, Context
from pydantic import Extra

from .named import Named
from .service_wrapper import WrapperType, WrapperHandler
from .state_tracker import StateTracker
from .types import (
    ServiceFunction,
    ServiceCondition,
    ACTOR,
    ServiceState,
    ConditionState,
)
from .service import Service
from .conditions import always_start_condition


_ServiceCallable = Union[Service, ServiceFunction, Literal[ACTOR]]

logger = logging.getLogger(__name__)


class ServiceGroup(StateTracker, Named, WrapperHandler):
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

    services: List[Union[_ServiceCallable, List[_ServiceCallable], "ServiceGroup"]]
    timeout: int = -1
    start_condition: ServiceCondition = always_start_condition

    class Config:
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.annotators: List[Union[Service, "ServiceGroup"]] = []

    async def _run(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        try:
            state = self.start_condition(ctx, actor)
            if state == ConditionState.ALLOWED:

                if self.asynchronous:
                    self._set_state(ctx, ServiceState.RUNNING)

                    running = dict()
                    for annotator in self.annotators:
                        if annotator._get_state(ctx, default=ServiceState.NOT_RUN) is ServiceState.NOT_RUN:
                            service_result = create_task(annotator(ctx, actor), name=annotator.name)
                            timeout = annotator.timeout if annotator.timeout > -1 else None
                            running.update({annotator.name: wait_for(service_result, timeout=timeout)})
                        self._set_state(ctx, None)

                    for name, future in zip(running.keys(), as_completed(running.values())):
                        try:
                            await future
                        except AsyncTimeoutError:
                            logger.warning(f"Service {name} timed out!")

                    failed = False
                    for annotator in self.annotators:
                        if annotator._get_state(ctx) == ServiceState.PENDING:
                            annotator._set_state(ctx, ServiceState.FAILED)
                            failed = True
                        self._set_state(ctx, None)
                    self._set_state(ctx, ServiceState.FAILED if failed else ServiceState.FINISHED)

                else:
                    for annotator in self.annotators:
                        service_result = None
                        if annotator.asynchronous:
                            timeout = annotator.timeout if annotator.timeout > -1 else None
                            task = create_task(annotator(ctx, actor, *args, **kwargs), name=annotator.name)
                            future = wait_for(task, timeout=timeout)
                            try:
                                await future
                            except AsyncTimeoutError:
                                logger.warning(f"{type(annotator).__name__} {annotator.name} timed out!")
                        else:
                            service_result = await annotator(ctx, actor)
                        if isinstance(service_result, Context):
                            ctx = service_result

                    self._set_state(ctx, ServiceState.FINISHED)

            elif state == ConditionState.PENDING:
                self._set_state(ctx, ServiceState.PENDING)
            else:
                self._set_state(ctx, ServiceState.FAILED)

        except Exception as e:
            self._set_state(ctx, ServiceState.FAILED)
            logger.error(f"Group {self.name} execution failed for unknown reason!\n{e}")

        return ctx

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        self._set_state(ctx, dict())
        self._execute_wrappers(ctx, actor, WrapperType.PREPROCESSING)

        timeout = self.timeout if self.timeout > -1 else None
        if self.asynchronous:
            task = create_task(self._run(ctx, actor, *args, **kwargs), name=self.name)
            future = wait_for(task, timeout=timeout)
            try:
                await future
            except AsyncTimeoutError:
                logger.warning(f"Group {self.name} timed out!")
        else:
            if timeout is not None:
                logger.warning(f"Timeout can not be applied for group {self.name}: it is not asynchronous !")
            ctx = await self._run(ctx, actor, *args, **kwargs)

        self._execute_wrappers(ctx, actor, WrapperType.POSTPROCESSING)
        return ctx

    @staticmethod
    def _get_name(
        group: Union[List[_ServiceCallable], "ServiceGroup"],
        forbidden_names: Optional[Set[str]] = None,
        name_rule: Optional[Callable[[Any], str]] = None,
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None,
    ) -> str:
        forbidden_names = forbidden_names if forbidden_names is not None else {"actor_", "func_", "obj_", "group_"}
        name_rule = name_rule if name_rule is not None else lambda this: "group"
        return super(ServiceGroup, ServiceGroup)._get_name(group, name_rule, forbidden_names, naming, given_name)

    def _recur_annotators(
        self, actor: Actor, naming: Optional[Dict[str, int]] = None
    ) -> List[Union[_ServiceCallable, List[_ServiceCallable], "ServiceGroup", Literal[ACTOR]]]:
        annotators = []
        for service in self.services:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                annotators.append(ServiceGroup.cast(service, actor, naming))
            else:
                annotators.append(Service.cast(service, naming))
        self.asynchronous = all([service.asynchronous for service in annotators])
        return annotators

    @classmethod
    def cast(
        cls,
        group: Union[List[_ServiceCallable], "ServiceGroup"],
        actor: Actor,
        naming: Optional[Dict[str, int]] = None,
        **kwargs,
    ) -> "ServiceGroup":
        """
        Method for service creation from actor, function or dict.
        No other sources are accepted (yet).
        """
        naming = {} if naming is None else naming
        if isinstance(group, ServiceGroup):
            group.name = cls._get_name(group, naming=naming, given_name=group.name)
            group.annotators = group._recur_annotators(actor, naming)
            return group
        elif isinstance(group, List):
            group = cls(services=group, name=cls._get_name(group, naming=naming), **kwargs)
            group.annotators = group._recur_annotators(actor, naming)
            return group
        else:
            raise Exception(f"Unknown type of group {group}")
