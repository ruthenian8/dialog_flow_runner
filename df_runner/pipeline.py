from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor

from df_runner import AbsProvider, Service, ServiceFunctionType, PipelineRunner
from df_runner.connector import DefaultConnector


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
        connector: DBAbstractConnector = DefaultConnector()
    ):
        self.provider = provider
        self.connector = connector
        self.services = []
        for service in services:
            if isinstance(service, Service):
                self.services.append(service)
            else:
                self.services.append(Service.create(service))

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
        runner = PipelineRunner(
            self.connector,
            self.provider,
            self.services
        )
        self.provider.run(runner)
