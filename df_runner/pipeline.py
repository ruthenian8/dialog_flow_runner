import logging
from typing import Union, List, Dict, TypedDict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor
from pydantic import BaseModel, Extra

from df_runner import AbsProvider, Service, ServiceFunction, CLIProvider, PipelineRunner, Wrapper, WrappedService, ServiceCondition
from df_runner.conditions import always_start_condition

logger = logging.getLogger(__name__)

_ServiceCallable = Union[Service, Actor, ServiceFunction]


class ServiceGroup(BaseModel):
    """
    An instance that represents a service group.
    Group can be also defined in pipeline dict as a nested service list.
    Instance of this class, however, provides possibility to explicitly define group name and wrapper classes for all group members.
    It accepts:
        name - custom group name (used for identification)
            NB! if name is not provided, it will be generated.
        services - a Service list in this group, may include Actor
        wrappers (optionally) - Wrapper classes array to add to all group services
    """

    name: Optional[str] = None
    services: List[_ServiceCallable]
    wrappers: Optional[List[Wrapper.__class__]] = None
    timeout: int = -1  # TODO: timeout
    start_condition: ServiceCondition = always_start_condition  # TODO: add with aggregation

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.wrappers = [] if self.wrappers is None else self.wrappers


class Pipeline(BaseModel):
    """
    Class that automates runner creation from dict and execution.
    It also allows actor and annotators wrapping with special services, which enables more control over execution.
    It accepts:
        services - a Service list for this pipeline, should include Actor
        provider (optionally) - an AbsProvider instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
        wrappers (optionally) - Wrapper classes array to add to all pipeline services
    """

    provider: Optional[AbsProvider] = CLIProvider()
    contex_db: Optional[Union[DBAbstractConnector, Dict]] = None
    services: List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup]] = None
    wrappers: Optional[List[Wrapper.__class__]] = None

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.contex_db = dict() if self.contex_db is None else self.contex_db
        self.wrappers = [] if self.wrappers is None else self.wrappers

        self._actor = None
        self._preprocessing = []
        self._postprocessing = []

        grouping = {}
        self._group_services(self.services, grouping=grouping, wrappers=self.wrappers)

        if self._actor is None:
            raise Exception("Incorrect pipeline description: missing actor")

        self._runner = PipelineRunner(self._actor, self.contex_db, self.provider, self._preprocessing, self._postprocessing, grouping)

    @staticmethod
    def _get_group_name(
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None
    ) -> str:
        """
        Method for name generation.
        Name is generated using following convention:
            'group_[NUMBER]'
        If user provided name uses same syntax it will be changed to auto-generated.
        """
        if given_name is not None and not (given_name.startswith('group_')):
            if naming is not None:
                if given_name in naming:
                    raise Exception(f"User defined group name collision: {given_name}")
                else:
                    naming[given_name] = True
            return given_name
        elif given_name is not None:
            logger.warning(f"User defined name for group '{given_name}' violates naming convention, the service will be renamed")

        if naming is not None:
            number = naming.get('group', 0)
            naming['group'] = number + 1
            return f'group_{number}'
        else:
            return given_name

    def _create_service(
        self,
        service: _ServiceCallable,
        naming: Dict[str, int],
        grouping: Dict[str, List[str]],
        groups: List[str],
        wrappers: Optional[List[Wrapper.__class__]] = None
    ):
        """
        Method for Service creation.
        Handles also wrappers creation for the service and adding it to specific groups.
        Keeps track of inner pipeline structure, separates preprocessors, postprocessors and actor.
        Raises an error in case of second actor appearance.
        """
        wrappers = [wrapper() for wrapper in wrappers]
        inst = WrappedService.cast(service, naming, groups=groups, wrappers=wrappers)

        if isinstance(inst.service, Actor):
            if self._actor is None:
                self._actor = inst
            else:
                raise Exception("Two actors defined for a pipeline!")
        elif self._actor is None:
            self._preprocessing.append(inst)
        else:
            self._postprocessing.append(inst)

        for group in groups:
            grouping[group].append(inst.name)

    def _group_services(
        self,
        group: Union[List[_ServiceCallable], ServiceGroup],
        naming: Optional[Dict[str, int]] = None,
        grouping: Optional[Dict[str, List[str]]] = None,
        groups: Optional[List[str]] = None,
        wrappers: Optional[List[Wrapper.__class__]] = None
    ):
        """
        Method for pipeline services creation.
        It iterates service groups recursively and creates string arrays of group names for each service.
        As a result, flat services lists are generated.
        It also creates a naming dict, which is used during service and group name generation.
        """
        wrappers = [] if wrappers is None else wrappers

        if isinstance(group, ServiceGroup):
            name = group.name
            wrappers = wrappers + group.wrappers
            group = group.services
        else:
            name = None

        if naming is not None:
            name = self._get_group_name(naming, name)
            grouping[name] = []
            groups = groups + [name]
        else:
            naming = {}
            groups = []

        for service in group:
            if isinstance(service, List) or isinstance(service, ServiceGroup):
                self._group_services(service, naming, grouping, groups, wrappers)
            else:
                self._create_service(service, naming, grouping, groups, wrappers)

    @property
    def processed_services(self) -> List[Service]:
        """
        Returns a copy of created inner services flat array used during actual pipeline running.
        Might be used for debugging purposes.
        """
        return self._runner.pre_annotators + [self._runner.actor] + self._runner.post_annotators

    def start(self):
        """
        Execute pipeline, an alias method for runner.start().
        """
        self._runner.start()


def create_pipelines(pipeline: TypedDict('ServiceDict', {
    'provider': Union[AbsProvider, List[AbsProvider]],
    'contex_db': Optional[DBAbstractConnector],
    'services': List[Union[Service, Actor, Dict, ServiceFunction]]
})):
    """
    TODO: requires refactoring, maybe add multiple providers support for one Pipeline (with async)
    TODO: ... needed anymore?
    """
    connector = pipeline['contex_db']
    providers = pipeline['provider'] if isinstance(pipeline['provider'], list) else [pipeline['provider']]
    return [Pipeline(provider=provider, services=pipeline['services'], connector=connector) for provider in providers]
