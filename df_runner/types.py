from abc import ABC
from enum import unique, Enum, auto
from typing import Callable, Any, Union, Awaitable, Dict, List, TypedDict, Optional, NewType, Iterable

from df_db_connector import DBAbstractConnector
from df_engine.core import Context, Actor
from typing_extensions import NotRequired


_ForwardPipe = NewType("Pipe", None)
_ForwardService = NewType("Service", _ForwardPipe)
_ForwardServiceGroup = NewType("ServiceGroup", _ForwardPipe)
_ForwardServiceWrapper = NewType("Wrapper", None)
_ForwardProvider = NewType("ABCProvider", ABC)


@unique
class PipeExecutionState(Enum):
    """
    Enum, representing service in a pipeline state.
    The following states are supported:
        NOT_RUN: service has not been executed yet (the default one)
        PENDING: service depends on other currently running service and can not be executed right now
        FINISHED: service executed successfully
        FAILED: service execution failed for some reason
    """

    NOT_RUN = auto()
    RUNNING = auto()
    FINISHED = auto()
    FAILED = auto()


@unique
class CallbackType(Enum):
    """
    Enum, representing types of callbacks, that can be set applied for a pipeline.
    The following types are supported:
        BEFORE_ALL: function called before each turn
        BEFORE: function called before each service
        AFTER: function called after each service
        AFTER_ALL: function called after each turn
    """

    BEFORE_ALL = auto()
    BEFORE = auto()
    AFTER = auto()
    AFTER_ALL = auto()


"""
RUNNER: storage for services and groups execution status
"""
RUNNER_STATE_KEY = "PIPELINE"


"""
A function type for attaching to different stages of pipeline execution.
Accepts string (service name) and any (service result, if any by that point).
"""
CallbackFunction = Callable[[str, Any], None]

"""
A function type for provider-to-client interaction.
Accepts string (user input), returns string (answer from pipeline).
"""
PipelineRunnerFunction = Callable[[Any, Any], Awaitable[Context]]

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
StartConditionCheckerFunction = Callable[[Context, Actor], bool]

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
StartConditionCheckerAggregationFunction = Callable[[Iterable[bool]], bool]

"""
A function type for creating wrappers (pre- and postprocessing).
Accepts context, actor (current pipeline state), name of the wrapped service and returns anything.
"""
WrapperFunction = Callable[[Context, Actor, str], Any]


PollingProviderLoopFunction = Callable[[], bool]


"""
A function type for creating services.
Accepts context, returns nothing.
"""
ServiceBuilder = Union[
    Callable[[Context, Actor], None],
    Callable[[Context, Actor], Awaitable[None]],
    _ForwardService,
    Actor,
    TypedDict(
        "ServiceDict",
        {
            "service_handler": "ServiceBuilder",
            "wrappers": NotRequired[Optional[List[_ForwardServiceWrapper]]],
            "timeout": NotRequired[Optional[int]],
            "asynchronous": NotRequired[bool],
            "start_condition": NotRequired[StartConditionCheckerFunction],
            "name": Optional[str],
        },
    ),
]

ServiceGroupBuilder = Union[
    List[Union[ServiceBuilder, List[ServiceBuilder], _ForwardServiceGroup]],
    TypedDict(
        "ServiceGroupDict",
        {
            "services": "ServiceGroupBuilder",
            "wrappers": NotRequired[Optional[List[_ForwardServiceWrapper]]],
            "timeout": NotRequired[Optional[int]],
            "asynchronous": NotRequired[bool],
            "start_condition": NotRequired[StartConditionCheckerFunction],
            "name": Optional[str],
        },
    ),
]

PipelineBuilder = TypedDict(
    "PipelineBuilder",
    {
        "provider": NotRequired[Optional[_ForwardProvider]],
        "context_db": NotRequired[Optional[Union[DBAbstractConnector, Dict]]],
        "services": ServiceGroupBuilder,
        "wrappers": NotRequired[Optional[List[_ForwardServiceWrapper]]],
        "optimization_warnings": NotRequired[bool],
    },
)
