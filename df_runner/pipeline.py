from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor
from pydantic import BaseModel, Extra

from df_runner import AbsProvider, Service, ServiceFunction, Runner, CLIProvider


_ServiceCallable = Union[Service, Actor, ServiceFunction]


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
    services: List[Union[_ServiceCallable, List[_ServiceCallable]]] = None

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.connector = dict() if self.connector is None else self.connector

        self._actor = None
        self._preprocessing = []
        self._postprocessing = []

        self._group_services(self.services)

        if self._actor is None:
            raise Exception("Incorrect pipeline description: missing actor")

        self._runner = Runner(self._actor, self.connector, self.provider, self._preprocessing, self._postprocessing)

    def _create_service(
        self,
        service: _ServiceCallable,
        naming: Dict[str, int],
        groups: List[str]
    ):
        inst = Service.cast(service, naming, groups=groups)
        if isinstance(inst.service, Actor):
            if self._actor is None:
                self._actor = inst
            else:
                raise Exception("Two actors defined for a pipeline!")
        elif self._actor is None:
            self._preprocessing.append(inst)
        else:
            self._postprocessing.append(inst)

    def _group_services(
        self,
        services: List[_ServiceCallable],
        naming: Optional[Dict[str, int]] = None,
        groups: Optional[List[str]] = None,
    ):
        if naming is not None:
            number = naming.get('group', 0)
            naming['group'] = number + 1
            groups = groups + [f'group-{number}']
        else:
            naming = {}
            groups = []

        for service in services:
            if isinstance(service, List):
                self._group_services(service, naming, groups)
            else:
                self._create_service(service, naming, groups)

    @property
    def processed_services(self):
        return self._runner._pre_annotators + [self._runner._actor] + self._runner._post_annotators

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
    return [Pipeline(provider=provider, services=pipeline['services'], connector=connector) for provider in providers]
