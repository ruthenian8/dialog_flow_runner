import uuid
from typing import Any, Optional, List, Callable

from df_engine.core import Context, Actor
from pydantic import BaseModel

from .types import FrameworkKeys, CallbackType, CallbackFunction
from .wrapper import WrapperType, Wrapper


class Runnable(BaseModel):
    name: Optional[str] = None
    asynchronous: bool = False
    wrappers: Optional[List[Wrapper]] = None

    def __init__(self, **kwargs):
        kwargs.update({"wrappers": kwargs.get("wrappers", [])})
        super().__init__(**kwargs)

    def retrieve_data(self, ctx: Context) -> Any:
        return ctx.framework_states[FrameworkKeys.SERVICES].get(self.name, None)

    def save_data(self, result: Any, ctx: Context):
        ctx.framework_states[FrameworkKeys.SERVICES][self.name] = result

    def save_wrapper_data(self, result: Any, ctx: Context, wrapper_name: str, wrapper_type: WrapperType):
        ctx.framework_states[FrameworkKeys.SERVICES_META][self.name] = result

    def add_callback_wrapper(
        self, callback_type: CallbackType, callback: CallbackFunction, condition: Callable[[str], bool] = lambda _: True
    ):
        before = callback_type is CallbackType.BEFORE_ALL or callback_type is CallbackType.BEFORE
        for_all = callback_type is CallbackType.BEFORE_ALL or callback_type is CallbackType.AFTER_ALL
        self.wrappers += [
            Wrapper.parse_obj(
                {
                    "name": f'{uuid.uuid4()}_{"before" if before else "after"}'
                    f'{"_all" if for_all else ""}_stats_wrapper',
                    f'{"pre" if before else "post"}_func': lambda c, _, n: callback(n, self.retrieve_data(c)),
                    f'{"post" if before else "pre"}_func': lambda _, __, ___: None,
                }
            )
        ]

    def _export_data(self, ctx: Context, actor: Actor, wrapper_type: WrapperType, result: Optional[Any] = None):
        if result is not None:
            self.save_data(result, ctx)
        for wrapper in self.wrappers:
            result = (
                wrapper.pre_func(ctx, actor, self.name)
                if wrapper_type is WrapperType.PREPROCESSING
                else wrapper.post_func(ctx, actor, self.name)
            )
            if result is not None:
                self.save_wrapper_data(result, ctx, wrapper.name, wrapper_type)

    def _framework_states_base(self, ctx: Context, key: FrameworkKeys, value: Any = None, default: Any = None) -> Any:
        if value is None:
            value = ctx.framework_states[key].get(self.name, default)
            if self.name in ctx.framework_states[key]:
                del ctx.framework_states[key][self.name]
            return value
        else:
            ctx.framework_states[key][self.name] = value

    def _framework_states_runner(self, ctx: Context, value: Any = None, default: Any = None) -> Any:
        return self._framework_states_base(ctx, FrameworkKeys.RUNNER, value, default)

    def _framework_states_service(self, ctx: Context, value: Any = None, default: Any = None) -> Any:
        return self._framework_states_base(ctx, FrameworkKeys.SERVICES, value, default)

    def _framework_states_meta(self, ctx: Context, value: Any = None, default: Any = None) -> Any:
        return self._framework_states_base(ctx, FrameworkKeys.SERVICES_META, value, default)
