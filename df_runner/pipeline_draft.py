from abc import ABC, abstractmethod
from ctypes import Union
from typing import Callable, Dict, List
from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Context
from pyparsing import Optional

# pipeline = {
#     "requester": telegram,
#     "db": db_connector,
#     "services": [
#           db_connector, 
#           telegram,
            # prepoc,
#         {
#             "service": prepoc,
#             "timeout": 1000,
#             "start_condition": condition.wait_ok(prepoc0),
#         },
#         actor,
#         cust_func,
#         {
#             "service": postproc,
#             # "name": "postproc",
#             "timeout": 1000,
#             "start_condition": condition,
#         },
#     ],
# }




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


class Service:
    def __init__(
        self,
        service: Union[Actor, AbsRequester, DBAbstractConnector, Callable],
        name: Optional[str] = None,
        timeout: int = 1000,
        start_condition: Callable = start_condition,
        is_actor: bool = False,
        is_preprocessing: bool = False,
        is_postprocessing: bool = False,
        is_processing: bool = False,
        is_db: bool = False,
        is_wrapper: bool = False,
        is_requester: bool = False,
    ):

        self.service = service
        self.name = repr(service) if name is None else name
        self.timeout = timeout
        self.start_condition = start_condition
        self.is_actor = is_actor
        self.is_preprocessing = is_preprocessing
        self.is_postprocessing = is_postprocessing
        self.is_processing = is_processing
        self.is_db = is_db
        self.is_wrapper = is_wrapper
        self.is_requester = is_requester

    @staticmethod
    def to_service(service: Union[Service, Actor, AbsRequester, DBAbstractConnector, Callable], timeout: int)->Service:
        if isinstance(service, Service):
            return service
        elif isinstance(service, Actor):
            return Service(service, timeout=timeout, is_actor=True)
        elif isinstance(service, AbsRequester):
            return Service(service, timeout=timeout, is_requester=True)
        elif isinstance(service, DBAbstractConnector):
            self.service = service
            self.name = repr(service)
            self.is_db = True
            self.type = "db"
        elif isinstance(service, Callable):
            self.service = service
            self.name = repr(service)
            self.is_processing = True
            self.type = "processing"


class Runner(AbsRunner):
    def __init__(
        self,
        requester: AbsRequester,
        services: List[Union[Actor, Callable, Dict, AbsRequester, DBAbstractConnector]],
        db: Optional[DBAbstractConnector] = None,
    ) -> None:
        pass

    def start(self, *args, **kwargs) -> None:
        while self.requester.loop(self):
            pass


# %%
class C1:
    pass


c1 = C1()
repr(c1)
# %%
