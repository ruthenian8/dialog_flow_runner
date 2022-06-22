from typing import Callable

from df_engine.core import Context, Actor


"""
A function type for provider-to-client interaction.
Accepts string (user input), returns string (answer from runner).
"""
ProviderFunction = Callable[[str], str]

"""
A function type for creating annotators (and also for creating services from).
Accepts context (before processing), returns context (after processing).
"""
ServiceFunction = Callable[[Context, Actor], Context]

"""
A function type for creating start_conditions for services.
Accepts context and actor (current pipeline state), returns boolean (whether service can be launched).
"""
ServiceCondition = Callable[[Context, Actor], bool]

"""
A function type for creating wrappers (pre- and postprocessing).
Accepts context and actor (current pipeline state) and returns nothing.
"""
WrapperFunction = Callable[[Context, Actor], None]
