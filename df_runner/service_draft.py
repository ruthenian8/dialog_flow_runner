"""
Context
---------------------------
Data structure which is used for the context storage.
It provides a convenient interface for working with data:
adding data, data serialization, type checking etc.

"""

from abc import ABC, abstractmethod
from ctypes import Union
from typing import Callable, Dict, List
from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Context
from pyparsing import Optional


from abc import ABC, abstractmethod
import logging
from uuid import UUID, uuid4

from typing import Any, Callable, Optional, Union

from pydantic import BaseModel, validate_arguments, Field, validator

logger = logging.getLogger(__name__)

Node = BaseModel


@validate_arguments
def sort_dict_keys(dictionary: dict) -> dict:
    """
    Sorting of keys in the `dictionary`.
    It is nesessary to do it after the deserialization: keys deserialize in a random order.
    """
    return {key: dictionary[key] for key in sorted(dictionary)}


class AbsRunner(ABC):
    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        raise NotImplementedError


class AbsRequester(ABC):
    @abstractmethod
    def loop(self, runner: AbsRunner) -> bool:
        raise NotImplementedError


def start_condition(*args, **kwargs) -> bool:
    return True


class AbsService(BaseModel):
    service: Union[Actor, AbsRequester, DBAbstractConnector, Callable]
    name: Optional[str] = None
    timeout: int = -1
    start_condition: Union[Callable, str] = start_condition
    is_actor: bool = False
    is_preprocessing: bool = False
    is_postprocessing: bool = False
    is_processing: bool = False
    is_db: bool = False
    is_wrapper: bool = False
    is_requester: bool = False

class Service(AbsService):
    @classmethod
    def cast(
        cls,
        service: Union[AbsService, Actor, AbsRequester, DBAbstractConnector, Callable],
        name: Optional[str] = None,
        timeout: int = -1,
        start_condition: Union[Callable, str] = start_condition,
        *args,
        **kwargs,
    ) -> AbsService:
        if isinstance(service, AbsService):
            return service
        # TODO: handling start_condition when it's str
        elif isinstance(service, Actor):
            return Service(service, name, timeout, start_condition, is_actor=True)
        elif isinstance(service, AbsRequester):
            return Service(service, name, timeout, start_condition, is_requester=True)
        elif isinstance(service, DBAbstractConnector):
            return Service(service, name, timeout, start_condition, is_db=True)
        elif isinstance(service, Callable):
            return Service(service, name, timeout, start_condition, is_processing=True)
        elif isinstance(service, dict):
            return Service.parse_obj(service)
        raise Exception(f"Unknown type of service {service}")