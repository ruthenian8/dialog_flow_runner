from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor
from pydantic import BaseModel, Extra

from df_runner import AbsProvider, Service, ServiceFunction, Runner, CLIProvider


class Pipeline(BaseModel):
    """
    Class that represents a pipeline of services with shared provider and connector.
    The services are executed sequentially with a shared context.
    It accepts:
        services - a Service list for this pipeline, should include Actor
        provider (optionally) - an AbsProvider instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
    """

    _provider: Optional[AbsProvider] = CLIProvider()
    _services: List[Union[Service, Actor, Dict, ServiceFunction]]
    _connector: Optional[Union[DBAbstractConnector, Dict]] = None

    class Config:
        fields = {'_provider': 'provider', '_services': 'services', '_connector': 'connector'}
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connector = dict() if self._connector is None else self._connector

        self._actor = None
        self._preprocessing = []
        self._postprocessing = []
        for service in self._services:
            if isinstance(service, Actor):
                self._actor = service
            else:
                inst = service if isinstance(service, Service) else Service.cast(service)
                if inst.is_actor:
                    self._actor = inst
                elif self._actor is None:
                    self._preprocessing.append(inst)
                else:
                    self._postprocessing.append(inst)

        if self._actor is None:
            raise Exception("Incorrect pipeline description: missing actor")

        # Here, self._actor is not necessarily an Actor instance, it may also be a Service instance (wrapping Actor) with the same callable interface, but please, don't tell anyone
        self._runner = Runner(self._actor, self._connector, self._provider, self._preprocessing, self._postprocessing)

    def run(self):
        """
        Execute pipeline.
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
