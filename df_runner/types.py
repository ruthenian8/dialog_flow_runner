from enum import unique, Enum
from typing import Callable, Any

from df_engine.core import Context, Actor


class ServiceState(Enum):
    NOT_RUN = 0
    WAITING = 1
    RUNNING = 2
    FINISHED = 3
    FAILED = 4


class ConditionState(Enum):
    ALLOWED = 0
    DENIED = 1
    WAITING = 2


"""
A function type for provider-to-client interaction.
Accepts string (user input), returns string (answer from runner).
"""
ProviderFunction = Callable[[Any], Context]

"""
A function type for creating annotators (and also for creating services from).
Accepts context (before processing), returns context (after processing).
"""
ServiceFunction = Callable[[Context, Actor], Context]

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
