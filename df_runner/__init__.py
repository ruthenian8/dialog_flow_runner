# -*- coding: utf-8 -*-
# flake8: noqa: F401

import nest_asyncio

nest_asyncio.apply()


from .types import (
    PipeExecutionState,
    CallbackType,
    WrapperStage,
    StartConditionCheckerFunction,
    ServiceInfo,
    WrapperInfo,
)

from .provider import CLIProvider, CallbackProvider
from .conditions import (
    always_start_condition,
    service_successful_condition,
    not_condition,
    aggregate_condition,
    all_condition,
    any_condition,
)

from .pipeline.component import Pipe
from .service.service import Service, wrap, add_wrapper
from .service.group import ServiceGroup
from .service.wrapper import Wrapper
from .pipeline.pipeline import Pipeline


__author__ = "Denis Kuznetsov"
__email__ = "kuznetsov.den.p@gmail.com"
__version__ = "0.2.1"
