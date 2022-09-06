import logging
import abc
import asyncio
import copy
import uuid
from typing import Optional, List, Union, Awaitable, Tuple, Any, Mapping

from df_engine.core import Context, Actor

from ..service.wrapper import Wrapper
from ..conditions import always_start_condition
from ..types import (
    PIPELINE_STATE_KEY,
    StartConditionCheckerFunction,
    ComponentExecutionState,
    ServiceRuntimeInfo,
    GlobalWrapperType,
    WrapperFunction,
)

logger = logging.getLogger(__name__)


class PipelineComponent(abc.ABC):
    """
    This class represents a pipeline component - a service or a service group.
    It contains some fields that they have in common:
        `wrappers` - list of Wrappers, associated with this component
        `timeout` (for asynchronous only!) - maximum component execution time (in seconds),
            if it exceeds this time, it is interrupted
        `requested_async_flag` - requested asynchronous property; if not defined, calculated_async_flag is used instead
        `calculated_async_flag` - whether the component can be asynchronous or not
            - for Service: whether its `handler` is asynchronous or not
            - for ServiceGroup: whether all its `services` are asynchronous or not
        `start_condition` - StartConditionCheckerFunction that is invoked before each component execution;
            component is executed only if it returns True
        `name` - component name (should be unique in single ServiceGroup), should not be blank or contain '.' symbol
        `path` - dot-separated path to component, is universally unique
    """

    def __init__(
        self,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        requested_async_flag: Optional[bool] = None,
        calculated_async_flag: bool = False,
        start_condition: Optional[StartConditionCheckerFunction] = None,
        name: Optional[str] = None,
        path: Optional[str] = None,
    ):
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self.requested_async_flag = requested_async_flag
        self.calculated_async_flag = calculated_async_flag
        self.start_condition = always_start_condition if start_condition is None else start_condition
        self.name = name
        self.path = path

        if name is not None and (name == "" or "." in name):
            raise Exception(f"User defined service name shouldn't be blank or contain '.' (service: {name})!")

        if not calculated_async_flag and requested_async_flag:
            raise Exception(f"{type(self).__name__} '{name}' can't be asynchronous!")

    def _get_attrs_with_updates(
        self,
        drop_attrs: Optional[Tuple[str, ...]] = None,
        replace_attrs: Optional[Mapping[str, str]] = None,
        add_attrs: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        """
        Advanced customizable version of built-in `__dict__` property.
        Sometimes during Pipeline construction Services (or ServiceGroups) should be rebuilt,
            e.g. in case of some fields overriding.
        This method can be customized to return a dict,
            that can be spread (** operator) and passed to Service or ServiceGroup constructor.
        Base dict is formed via `vars` built-in function. All "private" or "dunder" fields are omitted.
        :drop_attrs: - a tuple of key names that should be removed from the resulting dict.
        :replace_attrs: - a mapping, that should be replaced in the resulting dict.
        :add_attrs: - a mapping, that should be added to the resulting dict.
        Returns resulting dict.
        """
        drop_attrs = () if drop_attrs is None else drop_attrs
        replace_attrs = {} if replace_attrs is None else dict(replace_attrs)
        add_attrs = {} if add_attrs is None else dict(add_attrs)
        result = {}
        for attribute in vars(self):
            if not attribute.startswith("__") and attribute not in drop_attrs:
                if attribute in replace_attrs:
                    result[replace_attrs[attribute]] = getattr(self, attribute)
                else:
                    result[attribute] = getattr(self, attribute)
        result.update(add_attrs)
        return result

    def _set_state(self, ctx: Context, value: ComponentExecutionState):
        """
        Method for component runtime state setting, state is preserved in `ctx.framework_states` dict,
            in subdict, dedicated to this library.
        :ctx: - context to keep state in.
        :value: - state to set.
        Returns None.
        """
        if PIPELINE_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[PIPELINE_STATE_KEY] = {}
        ctx.framework_states[PIPELINE_STATE_KEY][self.path] = value.name

    def get_state(self, ctx: Context, default: Optional[ComponentExecutionState] = None) -> ComponentExecutionState:
        """
        Method for component runtime state getting, state is preserved in `ctx.framework_states` dict,
            in subdict, dedicated to this library.
        :ctx: (required) - context to get state from.
        :default: - default to return if no record found (usually it's `ComponentExecutionState.NOT_RUN`).
        Returns ComponentExecutionState of this service or default if not found.
        """
        return ComponentExecutionState[
            ctx.framework_states[PIPELINE_STATE_KEY].get(self.path, default if default is not None else None)
        ]

    @property
    def asynchronous(self) -> bool:
        """
        Property, that indicates, whether this component is synchronous or asynchronous.
        It is calculated according to following rule:
            1. If component can be asynchronous and `requested_async_flag` is set, it returns `requested_async_flag`
            2. If component can be asynchronous and `requested_async_flag` isn't set, it returns True
            3. If component can't be asynchronous and `requested_async_flag` is False or not set, it returns False
            4. If component can't be asynchronous and `requested_async_flag` is True,
                an Exception is thrown in constructor
        Returns bool.
        """
        return self.calculated_async_flag if self.requested_async_flag is None else self.requested_async_flag

    @abc.abstractmethod
    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        """
        A method for running pipeline component, it is overridden in all its children.
        This method is run after the component's timeout is set (if needed).
        :ctx: (required) - current dialog Context.
        :actor: - this Pipeline Actor or None if this is a service, that wraps Actor.
        Returns Context if this is a synchronous service or None, asynchronous services shouldn't modify Context.
        """
        raise NotImplementedError

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Union[Context, Awaitable]]:
        """
        A method for calling pipeline components.
        It sets up timeout if this component is asynchronous and executes it using `_run` method.
        :ctx: (required) - current dialog Context.
        :actor: - this Pipeline Actor or None if this is a service, that wraps Actor.
        Returns Context if this is a synchronous service or Awaitable if this is an asynchronous component or None.
        """
        if self.asynchronous:
            task = asyncio.create_task(self._run(ctx, actor), name=self.name)
            return asyncio.wait_for(task, timeout=self.timeout)
        else:
            return await self._run(ctx, actor)

    def add_wrapper(self, global_wrapper_type: GlobalWrapperType, wrapper: WrapperFunction):
        """
        Method for adding a global wrapper to this particular component.
        Wrapper is automatically named using `uuid.uuid4()`
            and depending on wrapper type for debugging/logging purposes.
        :global_wrapper_type: - a type of wrapper to add.
        :wrapper: - a WrapperFunction to add to the component as a wrapper.
        Returns None.
        """
        before = global_wrapper_type is GlobalWrapperType.BEFORE
        self.wrappers += [
            Wrapper(
                name=f'{uuid.uuid4()}_{"before" if before else "after"}_global_wrapper',
                before=wrapper if before else None,
                after=None if before else wrapper,
            )
        ]

    def _get_runtime_info(self, ctx: Context) -> ServiceRuntimeInfo:
        """
        Method for retrieving runtime info about this component.
        :ctx: - current dialog Context.
        Returns a ServiceRuntimeInfo dict where all not set fields are replaced with '[None]'.
        """
        return {
            "name": self.name if self.name is not None else "[None]",
            "path": self.path if self.path is not None else "[None]",
            "timeout": self.timeout,
            "asynchronous": self.asynchronous,
            "execution_state": copy.deepcopy(ctx.framework_states[PIPELINE_STATE_KEY]),
        }

    @property
    def info_dict(self) -> dict:
        """
        Property for retrieving info dictionary about this component.
        Returns info dict, containing most important component public fields as well as its type.
        All not set fields there are replaced with '[None]'.
        """
        return {
            "type": type(self).__name__,
            "name": self.name,
            "path": self.path if self.path is not None else "[None]",
            "asynchronous": self.asynchronous,
            "start_condition": self.start_condition.__name__,
            "wrappers": [wrapper.info_dict for wrapper in self.wrappers],
        }
