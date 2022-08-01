import uuid
from typing import Any, Optional, Dict, Callable

from df_engine.core import Context

from .types import FrameworkKeys
from .wrapper import WrapperType


class Runnable:
    name: Optional[str] = None
    asynchronous: bool = False

    def save_data(self, result: Any, ctx: Context):
        ctx.framework_states[FrameworkKeys.SERVICES][self.name] = result

    def save_wrapper_data(self, result: Any, ctx: Context, wrapper_name: str, wrapper_type: WrapperType):
        ctx.framework_states[FrameworkKeys.SERVICES_META][self.name] = result

    def _export_data(self, result: Any, ctx: Context, callback: Callable[[str, FrameworkKeys, Any], None]):
        self.save_data(result, ctx)
        callback(self.name, FrameworkKeys.SERVICES, result)

    def _export_wrapper_data(self, result: Any, ctx: Context, wrapper_name: str, wrapper_type: WrapperType, callback: Callable[[str, FrameworkKeys, Any], None]):
        self.save_wrapper_data(result, ctx, wrapper_name, wrapper_type)
        callback(f'{wrapper_name}', FrameworkKeys.SERVICES_META, result)
