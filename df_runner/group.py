import logging
from asyncio import wait_for, create_task, as_completed, TimeoutError as AsyncTimeoutError
from typing import Optional, List, Union, Dict, Literal, Callable, Any, Set

from df_engine.core import Actor, Context
from pydantic import BaseModel, Extra

from .named import Named
from .wrapper import Wrapper, WrapperType
from .runnable import Runnable
from .types import ServiceFunction, ServiceCondition, ACTOR, FrameworkKeys, ServiceState
from .service import Service
from .conditions import always_start_condition


_ServiceCallable = Union[Service, ServiceFunction]

logger = logging.getLogger(__name__)


class ServiceGroup(BaseModel, Runnable, Named):
    """
    An instance that represents a service group.
    Group can be also defined in pipeline dict as a nested service list.
    Instance of this class, however, provides possibility to explicitly define group name and wrapper classes for all group members.
    It accepts:
        name - custom group name (used for identification)
            NB! if name is not provided, it will be generated.
        services - a Service list in this group, may include Actor
        wrappers (optionally) - Wrapper classes array to add to all group services
    """

    name: Optional[str] = None
    services: List[Union[_ServiceCallable, List[_ServiceCallable], 'ServiceGroup', Literal[ACTOR]]]
    wrappers: Optional[List[Wrapper]] = None
    timeout: int = -1
    start_condition: ServiceCondition = always_start_condition

    class Config:
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.wrappers = [] if self.wrappers is None else self.wrappers
        self.asynchronous = False
        self.annotators: List[Union[Service, 'ServiceGroup']] = []

    async def _run(self, ctx: Context, callback: Callable[[str, FrameworkKeys, Any], None], actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        if self.asynchronous:
            self._framework_states_runner(ctx, ServiceState.RUNNING)

            running = dict()
            for annotator in self.annotators:
                if annotator._framework_states_runner(ctx, default=ServiceState.NOT_RUN) not in (ServiceState.NOT_RUN, ServiceState.PENDING):
                    service_result = create_task(annotator(ctx, callback, actor), name=annotator.name)
                    timeout = annotator.timeout if isinstance(annotator, Service) and annotator.timeout > -1 else None
                    running.update({annotator.name: wait_for(service_result, timeout=timeout)})

            for name, future in zip(running.keys(), as_completed(running.values())):
                try:
                    ctx = await future
                except AsyncTimeoutError as _:
                    logger.warning(f"Service {name} timed out!")

            failed = False
            for annotator in self.annotators:
                if annotator._framework_states_runner(ctx) == ServiceState.PENDING:
                    annotator._framework_states_runner(ctx, ServiceState.FAILED)
                    failed = True
            self._framework_states_runner(ctx, ServiceState.FAILED if failed else ServiceState.FINISHED)

        else:
            for annotator in self.annotators:
                service_result = None
                if annotator.asynchronous:
                    timeout = annotator.timeout if annotator.timeout > -1 else None
                    task = create_task(annotator(ctx, callback, actor, *args, **kwargs), name=annotator.name)
                    future = wait_for(task, timeout=timeout)
                    try:
                        await future
                    except AsyncTimeoutError as _:
                        logger.warning(f"Group {annotator.name} timed out!")
                else:
                    service_result = await annotator(ctx, callback, actor)
                if isinstance(service_result, Context):
                    ctx = service_result

            self._framework_states_runner(ctx, ServiceState.FINISHED)

        return ctx

    async def __call__(self, ctx: Context, callback: Callable[[str, FrameworkKeys, Any], None], actor: Optional[Actor] = None, *args, **kwargs) -> Optional[Context]:
        self._framework_states_runner(ctx, dict())
        for wrapper in self.wrappers:
            self._export_wrapper_data(wrapper.pre_func(ctx, actor), ctx, wrapper.name, WrapperType.PREPROCESSING, callback)

        timeout = self.timeout if self.timeout > -1 else None
        if self.asynchronous:
            task = create_task(self._run(ctx, callback, actor, *args, **kwargs), name=self.name)
            future = wait_for(task, timeout=timeout)
            try:
                await future
            except AsyncTimeoutError as _:
                logger.warning(f"Group {self.name} timed out!")
        else:
            if timeout is not None:
                logger.warning(f"Timeout can not be applied for group {self.name}: it is not asynchronous !")
            ctx = await self._run(ctx, callback, actor, *args, **kwargs)

        for wrapper in self.wrappers:
            self._export_wrapper_data(wrapper.post_func(ctx, actor), ctx, wrapper.name, WrapperType.POSTPROCESSING, callback)

        return ctx

    @staticmethod
    def _get_name(
        group: Union[List[_ServiceCallable], 'ServiceGroup'],
        forbidden_names: Optional[Set[str]] = None,
        name_rule: Optional[Callable[[Any], str]] = None,
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None
    ) -> str:
        forbidden_names = forbidden_names if forbidden_names is not None else {'actor_', 'func_', 'obj_', 'group_'}
        name_rule = name_rule if name_rule is not None else lambda this: 'group'
        return super(ServiceGroup, ServiceGroup)._get_name(group, name_rule, forbidden_names, naming, given_name)

    def _recur_annotators(
        self,
        actor: Actor,
        naming: Optional[Dict[str, int]] = None
    ) -> List[Union[_ServiceCallable, List[_ServiceCallable], 'ServiceGroup', Literal[ACTOR]]]:
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
        group: Union[List[_ServiceCallable], 'ServiceGroup'],
        actor: Actor,
        naming: Optional[Dict[str, int]] = None,
        **kwargs
    ) -> 'ServiceGroup':
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
            group = cls(
                services=group,
                name=cls._get_name(group, naming=naming),
                **kwargs
            )
            group.annotators = group._recur_annotators(actor, naming)
            return group
        else:
            raise Exception(f"Unknown type of group {group}")
