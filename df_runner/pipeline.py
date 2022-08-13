import logging
from typing import Union, List, Dict, TypedDict, Optional, Literal

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script
from df_engine.core.types import NodeLabel2Type
from pydantic import BaseModel, Extra

from .runner import Runner
from .wrapper import Wrapper
from .provider import AbsProvider, CLIProvider
from .group import ServiceGroup
from .types import ServiceFunction, ClearFunction, ACTOR, AnnotatorFunction, CallbackType, CallbackFunction
from .service import Service


logger = logging.getLogger(__name__)

_ServiceCallable = Union[Service, ServiceFunction]


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

    actor: Actor
    provider: Optional[AbsProvider] = CLIProvider()
    context_db: Optional[Union[DBAbstractConnector, Dict]] = None
    context_clear: Optional[ClearFunction] = None
    services: List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup, Literal[ACTOR]]] = None
    wrappers: Optional[List[Wrapper]] = None

    timeout: int = -1

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.context_db = dict() if self.context_db is None else self.context_db
        self.wrappers = [] if self.wrappers is None else self.wrappers

        self._annotators = ServiceGroup.cast(self.services, self.actor, dict(), wrappers=self.wrappers, timeout=self.timeout)
        self._runner = Runner(self.actor, self.context_clear, self.context_db, self.provider, self._annotators)

    def add_callback(self, callback_type: CallbackType, callback: CallbackFunction, whitelist: Optional[List[str]] = None, blacklist: Optional[List[str]] = None):
        def condition(name: str) -> bool:
            return (whitelist is not None and name in whitelist) and (blacklist is not None and name not in blacklist)

        self._annotators.add_callback_wrapper(callback_type, callback, condition)

    @property
    def processed_services(self) -> List[Service]:
        """
        Returns a copy of created inner services flat array used during actual pipeline running.
        Might be used for debugging purposes.
        FIXME: runtime flattening
        """
        return self._annotators

    def start_sync(self):
        """
        Execute pipeline, an alias method for runner.start_sync().
        """
        self._runner.start_sync()

    async def start_async(self):
        """
        Execute pipeline, an alias method for runner.start_async().
        """
        await self._runner.start_async()

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        request_provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[AnnotatorFunction]] = None,
        post_annotators: Optional[List[AnnotatorFunction]] = None
    ):
        actor = Actor(script, start_label, fallback_label)
        context_db = dict() if context_db is None else context_db
        pre_annotators = [] if pre_annotators is None else pre_annotators
        post_annotators = [] if post_annotators is None else post_annotators
        return cls(
            actor=actor,
            provider=request_provider,
            context_db=context_db,
            services=pre_annotators + [ACTOR] + post_annotators
        )


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
