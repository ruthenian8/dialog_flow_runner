from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor

from df_runner import AbsProvider, Service, ServiceFunctionType, Runner
from df_runner.connector import CLIConnector


ServiceDict = TypedDict('ServiceDict', {
    'provider': Union[AbsProvider, List[AbsProvider]],
    'connector': Optional[Union[DBAbstractConnector, List[DBAbstractConnector]]],
    'services': List[Union[Service, Actor, Dict, ServiceFunctionType]]
})


class Pipeline:
    def __init__(
        self,
        provider: AbsProvider,
        services: List[Union[Service, Actor, Dict, ServiceFunctionType]],
        connector: DBAbstractConnector = CLIConnector()
    ):
        self.provider = provider
        self.connector = connector

        self.actor = None
        self.preprocessing = []
        self.postprocessing = []
        for service in services:
            if isinstance(service, Actor):
                self.actor = service
            else:
                inst = service if isinstance(service, Service) else Service.create(service)
                if self.actor is None:
                    self.preprocessing.append(inst)
                else:
                    self.postprocessing.append(inst)

        self.runner = Runner(self.actor, self.connector, self.provider, self.preprocessing, self.postprocessing)

    @classmethod
    def create(cls, pipeline: ServiceDict):
        if 'connector' not in pipeline:
            connectors = []
        elif isinstance(pipeline['connector'], list):
            connectors = pipeline['connector']
        else:
            connectors = [pipeline['connector']]
        providers = pipeline['provider'] if isinstance(pipeline['provider'], list) else [pipeline['provider']]
        return [cls(provider, pipeline['services'], connector) for provider in providers for connector in connectors]

    def run(self):
        self.runner.start()
