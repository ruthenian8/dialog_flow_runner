# -*- coding: utf-8 -*-
# flake8: noqa: F401

import nest_asyncio

nest_asyncio.apply()


from .types import (
    PipelineRunnerFunction,
    StartConditionCheckerFunction,
    WrapperFunction,
    PipeExecutionState,
    RUNNER_STATE_KEY,
    CallbackType,
)

from .service_wrapper import Wrapper, WrapperStage
from .service import Service
from .service_group import ServiceGroup
from .provider import PollingProvider, CallbackProvider, CLIProvider
from .pipeline import Pipeline

__author__ = "Denis Kuznetsov"
__email__ = "kuznetsov.den.p@gmail.com"
__version__ = "0.2.1"

# TODO:
# Use stucture:
# - df_pipeline/
#   - pipeline/
#     - pipeline
#     - pipeline_component
#     - utils
#   - service/
#     - service
#     - service_group
#     - service_wrappers
#     - utils