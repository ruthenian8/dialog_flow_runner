# -*- coding: utf-8 -*-
# flake8: noqa: F401
from .types import ProviderFunction, ServiceFunction, ServiceCondition, WrapperFunction, ConditionState, ServiceState

from .provider import AbsProvider, PollingProvider, CallbackProvider, CLIProvider
from .service import Service
from .wrapper import Wrapper, WrappedService, wrap
from .runner import Runner, ScriptRunner, PipelineRunner
from .pipeline import Pipeline, ServiceGroup, create_pipelines


__author__ = "Denis Kuznetsov"
__email__ = "kuznetsov.den.p@gmail.com"
__version__ = "0.1.1"
