from enum import unique, Enum, auto
from typing import Callable, Any, Union, Awaitable, Dict, Tuple, List, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Context, Actor
from typing_extensions import NotRequired


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
    PENDING = auto()
    RUNNING = auto()
    FINISHED = auto()
    FAILED = auto()


@unique
class StartConditionState(Enum):
    """
    Enum, representing service condition state.
    The following states are supported:
        ALLOWED: service can be executed and all its conditions met
        DENIED: service can not be executed, some services it depends on failed
        PENDING: service can not be executed right now, some services it depends on are still running
    """

    ALLOWED = auto()
    DENIED = auto()


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
A function type for creating services.
Accepts context, returns nothing.
"""
ServiceBuilder = Union[Callable[[Context, Actor], None], Callable[[Context, Actor], Awaitable[None]], "Service", Actor, TypedDict("ServiceDict", {
        "service_handler": "ServiceBuilder",
        "wrappers": NotRequired[Optional[List["Wrapper"]]],
        "timeout": NotRequired[Optional[int]],
        "asynchronous": NotRequired[bool],
        "start_condition": NotRequired["ServiceCondition"],
        "name": Optional[str],
    },
)]

ServiceGroupBuilder = Union[List[Union[ServiceBuilder, List[ServiceBuilder], "ServiceGroup"]], TypedDict("ServiceGroupDict", {
        "services": "ServiceGroupBuilder",
        "wrappers": NotRequired[Optional[List["Wrapper"]]],
        "timeout": NotRequired[Optional[int]],
        "asynchronous": NotRequired[bool],
        "start_condition": NotRequired["ServiceCondition"],
        "name": Optional[str],
    },
)]

PipelineBuilder = TypedDict("PipelineBuilder", {
        "provider": NotRequired[Optional["AbsProvider"]],
        "context_db": NotRequired[Optional[Union[DBAbstractConnector, Dict]]],
        "services": ServiceGroupBuilder,
        "wrappers": NotRequired[Optional[List["Wrapper"]]],
    },
)

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
StartConditionCheckerFunction = Callable[[Context, Actor], StartConditionState]

"""
A function type for creating wrappers (pre- and postprocessing).
Accepts context, actor (current pipeline state), name of the wrapped service and returns anything.
"""
WrapperFunction = Callable[[Context, Actor, str], Any]


PollingProviderLoopFunction = Callable[[], bool]
