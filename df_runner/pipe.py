import logging
from abc import ABC, abstractmethod
from asyncio import create_task, wait_for
from typing import Any, Dict, Optional, List, Union, Awaitable

from df_engine.core import Context, Actor

from .service_wrapper import Wrapper
from .conditions import always_start_condition
from .types import RUNNER_STATE_KEY, StartConditionCheckerFunction


logger = logging.getLogger(__name__)


class Pipe(ABC):
    def __init__(
        self,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        asynchronous: bool = True,
        start_condition: StartConditionCheckerFunction = always_start_condition,
        name: Optional[str] = "pipe",
    ):
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self.asynchronous = asynchronous
        self.start_condition = start_condition
        self.name = name

    def _set_state(self, ctx: Context, value: Any = None):
        if RUNNER_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.framework_states[RUNNER_STATE_KEY][self.name] = value

    def _get_state(self, ctx: Context, default: Optional[Any] = None) -> Dict:
        return ctx.framework_states[RUNNER_STATE_KEY].get(self.name, {} if default is None else default)

    @abstractmethod
    async def _run(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Context]:
        pass

    def __call__(self, ctx: Context, actor: Optional[Actor] = None) -> Optional[Union[Context, Awaitable]]:
        timeout = self.timeout if self.timeout > -1 else None
        if self.asynchronous:
            timeout = self.timeout if self.timeout > -1 else None
            task = create_task(self._run(ctx, actor), name=self.name)
            return wait_for(task, timeout=timeout)
        else:
            if timeout is not None:
                logger.warning(f"Timeout can not be applied for {type(self).__name__} '{self.name}': it's not asynchronous!")
            return await self._run(ctx, actor)
