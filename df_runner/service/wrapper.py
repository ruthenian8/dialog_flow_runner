import logging
from inspect import signature
from typing import Optional

from df_engine.core import Context, Actor

from ..types import WrapperFunction, WrapperInfo, ServiceInfo, WrapperStage

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
        pre_func: Optional[WrapperFunction] = None,  # TODO: use same name handler
        post_func: Optional[WrapperFunction] = None,
        name: Optional[str] = None,
    ):
        self._pre_func = pre_func
        self._post_func = post_func
        self.name = name

    def _get_runtime_info(self, stage: WrapperStage, service_info: ServiceInfo) -> WrapperInfo:
        return {
            "name": self.name,
            "stage": stage,
            "service": service_info,
        }

    def run_wrapper_function(self, stage: WrapperStage, ctx: Context, actor: Actor, service_info: ServiceInfo):
        function = self._pre_func if stage is WrapperStage.PREPROCESSING else self._post_func
        if function is None:
            return

        handler_params = len(signature(function).parameters)
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
    def dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "name": self.name,
            "pre_func": self._pre_func.__name__,
            "post_func": self._post_func.__name__,
        }
