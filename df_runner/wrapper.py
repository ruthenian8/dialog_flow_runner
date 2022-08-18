import logging
from enum import unique, Enum, auto
from typing import Optional, Set, Callable, Any, Dict

from pydantic import BaseModel

from .named import Named
from .types import WrapperFunction


logger = logging.getLogger(__name__)


@unique
class WrapperType(Enum):
    """
    Enum, representing wrapper type, pre- or postprocessing.
    """

    PREPROCESSING = auto()
    POSTPROCESSING = auto()


class Wrapper(BaseModel, Named):
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after service.
    Wrappers should execute supportive tasks (like time or resources measurement).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    """

    pre_func: WrapperFunction
    post_func: WrapperFunction
    name: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = self._get_name(self, given_name=self.name)

    @staticmethod
    def _get_name(
        wrapper: "Wrapper",
        forbidden_names: Optional[Set[str]] = None,
        name_rule: Optional[Callable[[Any], str]] = None,
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None,
    ) -> str:
        forbidden_names = forbidden_names if forbidden_names is not None else {"wrapper_"}
        name_rule = name_rule if name_rule is not None else lambda this: f"wrapper_{type(this).__name__.lower()}"
        return super(Wrapper, Wrapper)._get_name(wrapper, name_rule, forbidden_names, naming, given_name)
