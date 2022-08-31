import logging
import inspect
from typing import Optional

from df_engine.core import Context, Actor

from ..types import WrapperFunction, WrapperRuntimeInfo, ServiceRuntimeInfo, WrapperStage

logger = logging.getLogger(__name__)


class Wrapper:
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after service.
    Wrappers should execute supportive tasks (like time or resources measurement).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    """

    def __init__(
        self,
        before: Optional[WrapperFunction] = None,
        after: Optional[WrapperFunction] = None,
        name: Optional[str] = None,
    ):
        self._before = before
        self._after = after
        self.name = name

    def _get_runtime_info(self, stage: WrapperStage, service_info: ServiceRuntimeInfo) -> WrapperRuntimeInfo:
        return {
            "name": self.name,
            "stage": stage,
            "service": service_info,
        }

    def run_stage(self, stage: WrapperStage, ctx: Context, actor: Actor, service_info: ServiceRuntimeInfo):
        function = self._before if stage is WrapperStage.PREPROCESSING else self._after
        if function is None:
            return

        handler_params = len(inspect.signature(function).parameters)
        if handler_params == 1:
            function(ctx)
        elif handler_params == 2:
            function(ctx, actor)
        elif handler_params == 3:
            function(ctx, actor, self._get_runtime_info(stage, service_info))
        else:
            raise Exception(
                f"Too many parameters required for wrapper `{self.name}` ({stage.name}) handler: {handler_params}!"
            )

    @property
    def info_dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "name": self.name,
            "pre_func": self._before.__name__,
            "post_func": self._after.__name__,
        }
