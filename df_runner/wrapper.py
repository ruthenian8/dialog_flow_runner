import logging
from enum import unique, Enum, auto
from typing import Optional

from pydantic import BaseModel

from df_runner import WrapperFunction


logger = logging.getLogger(__name__)


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
    name: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.name = self._get_name(self.name)

    def _get_name(
        self,
        given_name: Optional[str] = None
    ) -> str:
        """
        Method for name generation.
        Name is generated using following convention:
            actor: 'actor_[NUMBER]'
            function: 'func_[REPR]_[NUMBER]'
            service object: 'obj_[NUMBER]'
        If user provided name uses same syntax it will be changed to auto-generated.
        """
        if given_name is not None and not (given_name.startswith('wrapper_')):
            return given_name
        elif given_name is not None:
            logger.warning(f"User defined name for service '{given_name}' violates naming convention, the service will be renamed")

        return f'wrapper_{type(self).__name__.lower()}'
