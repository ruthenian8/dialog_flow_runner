import logging
import inspect
from typing import Optional

from df_engine.core import Context, Actor

from ..types import WrapperFunction, WrapperRuntimeInfo, ServiceRuntimeInfo, WrapperStage

logger = logging.getLogger(__name__)


class Wrapper:
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after pipeline component.
    Wrappers should execute supportive tasks (like time or resources measurement, minor data transformations).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    It accepts constructor parameters:
        `before` - function to be executed before component
        `after` - function to be executed after component
        `name` - wrapper name
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

    def _get_runtime_info(self, stage: WrapperStage, component_info: ServiceRuntimeInfo) -> WrapperRuntimeInfo:
        """
        Method for retrieving runtime info about this wrapper.
        It embeds runtime info of the component it wraps under `component` key.
        :stage: - current WrapperStage (before or after).
        :component_info: - associated component's info dictionary.
        Returns a WrapperRuntimeInfo dict where all not set fields are replaced with '[None]'.
        """
        return {
            "name": self.name,
            "stage": stage,
            "component": component_info,
        }

    def run_stage(self, stage: WrapperStage, ctx: Context, actor: Actor, component_info: ServiceRuntimeInfo):
        """
        Method for executing one of the wrapper functions (before or after).
        If the function is not set, nothing happens.
        :stage: - current WrapperStage (before or after).
        :ctx: - current dialog context.
        :actor: - actor, associated with current pipeline.
        :component_info: - associated component's info dictionary.
        Returns None.
        """
        function = self._before if stage is WrapperStage.PREPROCESSING else self._after
        if function is None:
            return

        handler_params = len(inspect.signature(function).parameters)
        if handler_params == 1:
            function(ctx)
        elif handler_params == 2:
            function(ctx, actor)
        elif handler_params == 3:
            function(ctx, actor, self._get_runtime_info(stage, component_info))
        else:
            raise Exception(
                f"Too many parameters required for wrapper `{self.name}` ({stage.name}) handler: {handler_params}!"
            )

    @property
    def info_dict(self) -> dict:
        """
        Property for retrieving info dictionary about this wrapper.
        Returns info dict, containing its fields as well as its type.
        All not set fields there are replaced with '[None]'.
        """
        return {
            "type": type(self).__name__,
            "name": self.name,
            "before": "[None]" if self._before is None else self._before.__name__,
            "after": "[None]" if self._after is None else self._after.__name__,
        }
