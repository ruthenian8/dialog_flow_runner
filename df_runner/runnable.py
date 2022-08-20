import uuid
from typing import Any, Optional, List, Callable

from df_engine.core import Context, Actor
from pydantic import BaseModel

from .types import FrameworkKeys, CallbackType, CallbackFunction
from .service_wrapper import WrapperType, Wrapper


class Runnable(BaseModel):
    name: Optional[str] = None
    asynchronous: bool = False
    wrappers: Optional[List[Wrapper]] = None

    def __init__(self, **kwargs):
        kwargs.update({"wrappers": kwargs.get("wrappers", [])})
        super().__init__(**kwargs)

    def _execure_service_wrapper(self, ctx: Context, actor: Actor, wrapper_type: WrapperType, result: Optional[Any] = None):
        for wrapper in self.wrappers:
            if wrapper_type is WrapperType.PREPROCESSING:
                wrapper.pre_func(ctx, actor, self.name)
            else:
                wrapper.post_func(ctx, actor, self.name)

    def _framework_states_runner(self, ctx: Context, value: Any = None, default: Any = None) -> Any:
        if value is None:
            value = ctx.framework_states[FrameworkKeys.RUNNER].get(self.name, default)
            if self.name in ctx.framework_states[FrameworkKeys.RUNNER]:
                del ctx.framework_states[FrameworkKeys.RUNNER][self.name]
            return value
        else:
            ctx.framework_states[FrameworkKeys.RUNNER][self.name] = value
