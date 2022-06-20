import logging
from typing import Callable, Optional, Union

from df_engine.core import Actor, Context
from pydantic import BaseModel, validate_arguments

from df_runner import ServiceFunctionType, ServiceConditionType

logger = logging.getLogger(__name__)


@validate_arguments
def _sort_dict_keys(dictionary: dict) -> dict:
    """
    Sorting of keys in the `dictionary`.
    It is necessary to do it after the deserialization: keys deserialize in a random order.
    """
    return {key: dictionary[key] for key in sorted(dictionary)}


def _default_start_condition(ctx: Context, actor: Actor) -> (Context, bool):
    return ctx, True


class Service(BaseModel):
    def __init__(
        self,
        service: Union[Actor, ServiceFunctionType],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: ServiceConditionType = _default_start_condition,
        is_actor: bool = False
    ):
        super().__init__(service, name, timeout, start_condition, is_actor)
        self.service = service
        self.name = repr(service) if name is None else name
        self.timeout = timeout
        self.start_condition = start_condition
        self.is_actor = is_actor

    @classmethod
    def create(
        cls,
        service: Union[Actor, ServiceFunctionType],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: ServiceConditionType = _default_start_condition,
    ):
        # TODO: handling start_condition when it's str -> what does it mean??
        if isinstance(service, Actor):
            return cls(service, name, timeout, start_condition, is_actor=True)
        elif isinstance(service, Callable):
            return cls(service, name, timeout, start_condition, is_actor=False)
        elif isinstance(service, dict):
            return cls.parse_obj(service)
        raise Exception(f"Unknown type of service {service}")
