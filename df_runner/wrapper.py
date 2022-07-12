from enum import unique, Enum, auto

from pydantic import BaseModel

from df_runner import WrapperFunction


@unique
class WrapperType(Enum):
    """
    Enum, representing wrapper type, pre- or postprocessing.
    """

    PREPROCESSING = auto()
    POSTPROCESSING = auto()


class Wrapper(BaseModel):
    """
    Class, representing a wrapper.
    A wrapper is a set of two functions, one run before and one after service.
    Wrappers should execute supportive tasks (like time or resources measurement).
    Wrappers should NOT edit context or actor, use services for that purpose instead.
    """

    pre_func: WrapperFunction
    post_func: WrapperFunction
