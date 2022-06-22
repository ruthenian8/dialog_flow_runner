from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor
from pydantic import BaseModel, Extra

from df_runner import AbsProvider, Service, ServiceFunction, Runner, CLIProvider


class Pipeline(BaseModel):
    """
    Class that automates runner creation from dict and execution.
    It also allows actor and annotators wrapping with special services, which enables more control over execution.
    It accepts:
        services - a Service list for this pipeline, should include Actor
        provider (optionally) - an AbsProvider instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
    """

    provider: Optional[AbsProvider] = CLIProvider()
    connector: Optional[Union[DBAbstractConnector, Dict]] = None
    services: List[Union[Service, Actor, Dict, ServiceFunction]] = None

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connector = dict() if self.connector is None else self.connector

        self._actor = None
        self._preprocessing = []
        self._postprocessing = []
        for service in self.services:
            inst = service if isinstance(service, Service) else Service.cast(service)
            if isinstance(inst.service, Actor):
                self._actor = inst
            elif self._actor is None:
                self._preprocessing.append(inst)
            else:
                self._postprocessing.append(inst)

        if self._actor is None:
            raise Exception("Incorrect pipeline description: missing actor")

        self._runner = Runner(self._actor, self.connector, self.provider, self._preprocessing, self._postprocessing)

    def start(self):
        """
        Execute pipeline, an alias method for runner.start().
        TODO: add threading policy here (especially in case of multiple runners)
        """
        self._runner.start()


def create_pipelines(pipeline: TypedDict('ServiceDict', {
    'provider': Union[AbsProvider, List[AbsProvider]],
    'connector': Optional[DBAbstractConnector],
    'services': List[Union[Service, Actor, Dict, ServiceFunction]]
})):
    """
    TODO: requires refactoring, maybe add multiple providers support for one Pipeline (with async)
    """
    connector = pipeline['connector']
    providers = pipeline['provider'] if isinstance(pipeline['provider'], list) else [pipeline['provider']]
    return [Pipeline(provider, pipeline['services'], connector) for provider in providers]
