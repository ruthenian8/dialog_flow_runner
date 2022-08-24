import logging
from abc import ABC, abstractmethod
from asyncio import create_task, wait_for
from typing import Optional, List, Union, Awaitable

from df_engine.core import Context, Actor

from .service_wrapper import Wrapper
from .conditions import always_start_condition
from .types import RUNNER_STATE_KEY, StartConditionCheckerFunction, PipeExecutionState


logger = logging.getLogger(__name__)


class Pipe(ABC):
    def __init__(
        self,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: Optional[int] = None,
        user_async: Optional[bool] = None,
        calc_async: bool = False,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = "pipe",
    ):
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self._user_async = user_async
        self._calc_async = calc_async
        self.start_condition = start_condition
        self.name = name

    def _set_state(self, ctx: Context, value: PipeExecutionState):
        if RUNNER_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.framework_states[RUNNER_STATE_KEY][self.name] = value

    def _get_state(self, ctx: Context, default: Optional[PipeExecutionState] = None) -> PipeExecutionState:
        return ctx.framework_states[RUNNER_STATE_KEY].get(self.name, default)

    @property
    def asynchronous(self):
        return self._calc_async if self._user_async is None else self._user_async

    @abstractmethod
    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        pass

    async def __call__(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Union[Context, Awaitable]]:
        if self.asynchronous:
            task = create_task(self._run(ctx, actor), name=self.name)
            return wait_for(task, timeout=self.timeout)
        else:
            return await self._run(ctx, actor)

    def to_string(self, show_wrappers: bool = False, offset: str = "") -> str:
        representation = f"{offset}{type(self).__name__} '{self.name}':\n"
        representation += f"{offset}\ttimeout: {self.timeout}\n"
        representation += f"{offset}\tasynchronous: {self.asynchronous}\n"
        representation += f"{offset}\tstart_condition: {self.start_condition.__name__}\n"
        if show_wrappers:
            if len(self.wrappers) > 0:
                wrappers_list = [wrapper.to_string(f"\t\t{offset}") for wrapper in self.wrappers]
                wrappers_representation = f"\n%s" % "\n".join(wrappers_list)
            else:
                wrappers_representation = "[None]"
            representation += f"{offset}\twrappers: {wrappers_representation}\n"
        return representation
