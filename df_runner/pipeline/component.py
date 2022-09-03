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
    def __init__(
        self,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        requested_async_flag: Optional[bool] = None,
        calculated_async_flag: bool = False,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = None,
        path: Optional[str] = None,
    ):
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self.requested_async_flag = requested_async_flag
        self.calculated_async_flag = calculated_async_flag
        self.start_condition = start_condition
        self.name = name
        self.path = path

        if name is not None and (name == "" or "." in name):
            raise Exception(f"User defined service name shouldn't be blank or contain '.' (service: {name})!")

    def get_attrs_with_updates(
        self,
        drop_attrs: Optional[Tuple[str, ...]] = None,
        replace_attrs: Optional[Mapping[str, str]] = None,
        add_attrs: Optional[Mapping[str, Any]] = None,
    ) -> dict:
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
        if PIPELINE_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[PIPELINE_STATE_KEY] = {}
        ctx.framework_states[PIPELINE_STATE_KEY][self.path] = value.name

    def get_state(self, ctx: Context, default: Optional[ComponentExecutionState] = None) -> ComponentExecutionState:
        return ComponentExecutionState[
            ctx.framework_states[PIPELINE_STATE_KEY].get(self.path, default if default is not None else None)
        ]

    @property
    def asynchronous(self) -> bool:
        return self.calculated_async_flag if self.requested_async_flag is None else self.requested_async_flag

    @abc.abstractmethod
    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        raise NotImplementedError

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Union[Context, Awaitable]]:
        if self.asynchronous:
            task = asyncio.create_task(self._run(ctx, actor), name=self.name)
            return asyncio.wait_for(task, timeout=self.timeout)
        else:
            return await self._run(ctx, actor)

    def add_wrapper(self, global_wrapper_type: GlobalWrapperType, wrapper: WrapperFunction):
        before = global_wrapper_type is GlobalWrapperType.BEFORE
        self.wrappers += [
            Wrapper(
                name=f'{uuid.uuid4()}_{"before" if before else "after"}_stats_wrapper',
                before=wrapper if before else None,
                after=None if before else wrapper,
            )
        ]

    def _get_runtime_info(self, ctx: Context) -> ServiceRuntimeInfo:
        return {
            "name": self.name if self.name is not None else "[None]",
            "path": self.path if self.path is not None else "[None]",
            "timeout": self.timeout,
            "asynchronous": self.asynchronous,
            "execution_state": copy.deepcopy(ctx.framework_states[PIPELINE_STATE_KEY]),
        }

    @property
    def info_dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "name": self.name,
            "path": self.path if self.path is not None else "[None]",
            "asynchronous": self.asynchronous,
            "start_condition": self.start_condition.__name__,
            "wrappers": [wrapper.info_dict for wrapper in self.wrappers],
        }
