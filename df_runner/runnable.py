from typing import Any, Optional

from df_engine.core import Context

from .types import FrameworkKeys
from .wrapper import WrapperType


class Runnable:
    name: Optional[str] = None
    asynchronous: bool = False

    def _export_data(self, result: Any, ctx: Context):
        ctx.framework_states[FrameworkKeys.SERVICES][self.name] = result

    def _export_wrapper_data(self, result: Any, ctx: Context, wrapper_name: str, wrapper_type: WrapperType):
        ctx.framework_states[FrameworkKeys.SERVICES_META][self.name] = result
