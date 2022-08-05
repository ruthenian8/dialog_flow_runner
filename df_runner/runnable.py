from typing import Any, Optional

from df_engine.core import Context, Actor

from .types import FrameworkKeys, CallbackInternalFunction, CallbackType
from .wrapper import WrapperType, Wrapper


class Runnable:
    name: Optional[str] = None
    asynchronous: bool = False

    def save_data(self, result: Any, ctx: Context):
        ctx.framework_states[FrameworkKeys.SERVICES][self.name] = result

    def save_wrapper_data(self, result: Any, ctx: Context, wrapper_name: str, wrapper_type: WrapperType):
        ctx.framework_states[FrameworkKeys.SERVICES_META][self.name] = result

    def _export_data(self, result: Any, ctx: Context, call_type: CallbackType, callback: CallbackInternalFunction):
        self.save_data(result, ctx)
        callback(self.name, call_type, FrameworkKeys.SERVICES, result)

    def _export_wrapper_data(self, wrapper: Wrapper, ctx: Context, actor: Actor, wrapper_type: WrapperType, callback: CallbackInternalFunction):
        callback(wrapper.name, CallbackType.BEFORE, FrameworkKeys.SERVICES_META, None)
        result = wrapper.pre_func(ctx, actor) if WrapperType.PREPROCESSING else wrapper.post_func(ctx, actor)
        self.save_wrapper_data(result, ctx, wrapper.name, wrapper_type)
        callback(wrapper.name, CallbackType.AFTER, FrameworkKeys.SERVICES_META, result)

    def _framework_states_base(self, ctx: Context, key: FrameworkKeys, value: Any = None, default: Any = None) -> Any:
        if value is None:
            value = ctx.framework_states[key].get(self.name, default)
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
