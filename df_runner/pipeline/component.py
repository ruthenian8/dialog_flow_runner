import logging
from abc import ABC, abstractmethod
from asyncio import create_task, wait_for
from copy import deepcopy
from typing import Optional, List, Union, Awaitable, Tuple, Any

from df_engine.core import Context, Actor

from ..service.wrapper import Wrapper
from ..conditions import always_start_condition
from ..types import RUNNER_STATE_KEY, StartConditionCheckerFunction, PipeExecutionState, ServiceInfo

logger = logging.getLogger(__name__)


class Pipe(ABC):
    def __init__(
        self,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        requested_async_flag: Optional[bool] = None,
        calculated_async_flag: bool = False,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: str = "pipe",
    ):
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self.requested_async_flag = requested_async_flag
        self.calculated_async_flag = calculated_async_flag
        self.start_condition = start_condition
        self.name = name

    def decay(
        self,
        drop_attrs: Tuple[str, ...] = (),
        replace_attrs: Tuple[Tuple[str, str], ...] = (),
        add_attrs: Tuple[Tuple[str, Any], ...] = (),
    ) -> dict:
        replace_attrs = dict(replace_attrs)
        result = {}
        for attribute in vars(self):
            if not attribute.startswith("_") and attribute not in drop_attrs:
                if attribute in replace_attrs:
                    result[replace_attrs[attribute]] = getattr(self, attribute)
                else:
                    result[attribute] = getattr(self, attribute)
        result.update(dict(add_attrs))
        return result

    def _set_state(self, ctx: Context, value: PipeExecutionState):
        if RUNNER_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.framework_states[RUNNER_STATE_KEY][self.name] = value

    def _get_state(self, ctx: Context, default: Optional[PipeExecutionState] = None) -> PipeExecutionState:
        return ctx.framework_states[RUNNER_STATE_KEY].get(self.name, default)

    @property
    def asynchronous(self) -> bool:
        return self.calculated_async_flag if self.requested_async_flag is None else self.requested_async_flag

    @abstractmethod
    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        raise NotImplementedError

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Union[Context, Awaitable]]:
        if self.asynchronous:
            task = create_task(self._run(ctx, actor), name=self.name)
            return wait_for(task, timeout=self.timeout)
        else:
            return await self._run(ctx, actor)

    def _get_runtime_info(self, ctx: Context) -> ServiceInfo:
        return {
            "name": self.name,
            "timeout": self.timeout,
            "asynchronous": self.asynchronous,
            "execution_state": deepcopy(ctx.framework_states[RUNNER_STATE_KEY]),
        }

    @property
    def dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "name": self.name,
            "asynchronous": self.asynchronous,
            "start_condition": self.start_condition.__name__,
            "wrappers": [wrapper.dict for wrapper in self.wrappers],
        }
