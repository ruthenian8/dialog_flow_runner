from enum import unique, Enum, auto
from typing import Callable, Any, Union, Awaitable, Dict, Tuple

from df_engine.core import Context, Actor


@unique
class ServiceState(Enum):
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
class ConditionState(Enum):
    """
    Enum, representing service condition state.
    The following states are supported:
        ALLOWED: service can be executed and all its conditions met
        DENIED: service can not be executed, some services it depends on failed
        PENDING: service can not be executed right now, some services it depends on are still running
    """

    ALLOWED = auto()
    DENIED = auto()
    PENDING = auto()


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
RUNNER_STATE_KEY = "RUNNER"


"""
A function type for attaching to different stages of pipeline execution.
Accepts string (service name) and any (service result, if any by that point).
"""
CallbackFunction = Callable[[str, Any], None]

"""
A function type for provider-to-client interaction.
Accepts string (user input), returns string (answer from pipeline).
"""
ProviderFunction = Callable[[Any, Any], Awaitable[Context]]

"""
A function type for creating services.
Accepts context, returns nothing.
"""
Handler = Union[Callable[[Context, Actor], None], Callable[[Context, Actor], Awaitable[None]]]

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
ServiceCondition = Callable[[Context, Actor], ConditionState]

"""
A function type for creating wrappers (pre- and postprocessing).
Accepts context, actor (current pipeline state), name of the wrapped service and returns anything.
"""
WrapperFunction = Callable[[Context, Actor, str], Any]


ClearFunction = Callable[[Dict, Dict], Tuple[Dict, Dict]]


LoopFunction = Callable[[], bool]
