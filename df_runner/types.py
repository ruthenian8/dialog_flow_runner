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

    NOT_RUN = 0
    PENDING = 1
    RUNNING = 2
    FINISHED = 3
    FAILED = 4


@unique
class ConditionState(Enum):
    """
    Enum, representing service condition state.
    The following states are supported:
        ALLOWED: service can be executed and all its conditions met
        DENIED: service can not be executed, some services it depends on failed
        PENDING: service can not be executed right now, some services it depends on are still running
    """

    ALLOWED = 0
    DENIED = 1
    PENDING = 2


@unique
class FrameworkKeys(Enum):
    """
    Enum, representing service condition state.
    The following states are supported:
        ALLOWED: service can be executed and all its conditions met
        DENIED: service can not be executed, some services it depends on failed
        PENDING: service can not be executed right now, some services it depends on are still running
    """

    RUNNER = auto()
    SERVICES = auto()
    SERVICES_META = auto()


ACTOR = 'ACTOR'


"""
A function type for provider-to-client interaction.
Accepts string (user input), returns string (answer from runner).
"""
ProviderFunction = Callable[[Any], Awaitable[Context]]

"""
A function type for creating annotators (and also for creating services from).
Accepts context (before processing), returns context (after processing).
"""
AnnotatorFunction = Union[Callable[[Context, Actor], Context], Callable[[Context, Actor], Awaitable[Context]]]

"""
A function type for creating services.
Accepts context, returns anything (will be written to context).
"""
ServiceFunction = Union[Callable[[Context, Actor], Any], Callable[[Context, Actor], Awaitable[Any]]]

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
ServiceCondition = Callable[[Context, Actor], ConditionState]

"""
A function type for creating wrappers (pre- and postprocessing).
Accepts context and actor (current pipeline state) and returns nothing.
"""
WrapperFunction = Callable[[Context, Actor], None]


ClearFunction = Callable[[Dict, Dict], Tuple[Dict, Dict]]
