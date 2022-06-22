# -*- coding: utf-8 -*-
# flake8: noqa: F401
from .types import ServiceFunction, ServiceCondition, WrapperFunction

from .provider import AbsProvider, CLIProvider
from .conditions import always_start_condition, service_successful_condition
from .service import Service
from .runner import Runner, ScriptRunner
from .pipeline import Pipeline, create_pipelines


__author__ = "Denis Kuznetsov"
__email__ = "kuznetsov.den.p@gmail.com"
__version__ = "0.1.1"
