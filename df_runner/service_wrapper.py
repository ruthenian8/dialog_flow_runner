import logging
from enum import unique, Enum, auto
from typing import Optional, List


from df_engine.core import Context, Actor

from .types import WrapperFunction


logger = logging.getLogger(__name__)


@unique
class WrapperStage(Enum):
    """
    Enum, representing wrapper type, pre- or postprocessing.
    """

    PREPROCESSING = auto()
    POSTPROCESSING = auto()


class Wrapper:
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after service.
    Wrappers should execute supportive tasks (like time or resources measurement).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    """

    def __init__(
        self,
        pre_func: WrapperFunction,
        post_func: WrapperFunction,
        name: Optional[str] = None,
    ):
        self.pre_func = pre_func
        self.post_func = post_func
        self.name = name

    def to_string(self, offset: str = "") -> str:
        representation = f"{offset}{type(self).__name__} '{self.name}':\n"
        representation += f"{offset}\tpre_func: Callable '{self.pre_func.__name__}'\n"
        representation += f"{offset}\tpost_func: Callable '{self.post_func.__name__}'"
        return representation


def execute_wrappers(ctx: Context, actor: Actor, wrappers: List[Wrapper], wrapper_type: WrapperStage, name):
    for wrapper in wrappers:
        if wrapper_type is WrapperStage.PREPROCESSING:
            wrapper.pre_func(ctx, actor, name)
        else:
            wrapper.post_func(ctx, actor, name)
