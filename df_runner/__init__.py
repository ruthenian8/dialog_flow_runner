# -*- coding: utf-8 -*-
# flake8: noqa: F401
from .types import ServiceFunctionType, ServiceConditionType, AnnotatorFunctionType

from .provider import AbsProvider, CLIProvider
from .service import Service
from .runner import Runner, ScriptRunner
from .pipeline import Pipeline


__author__ = "Denis Kuznetsov"
__email__ = "kuznetsov.den.p@gmail.com"
__version__ = "0.1.1"
