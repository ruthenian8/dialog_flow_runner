from contextvars import Context
import logging
from typing import Union, List, Dict, TypedDict, Optional, Literal, Tuple
import uuid
from asyncio import run

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script
from df_engine.core.types import NodeLabel2Type
from pydantic import BaseModel, Extra
from typing_extensions import NotRequired

from .runner import Runner
from .service_wrapper import Wrapper
from .provider import AbsProvider, CLIProvider
from .service_group import ServiceGroup
from .types import ServiceFunction, ClearFunction, ACTOR, AnnotatorFunction
from .service import Service


logger = logging.getLogger(__name__)

_ServiceCallable = Union[Service, ServiceFunction, Literal[ACTOR]]

PipelineDict = TypedDict(
    "PipelineDict",
    {
        "actor": Actor,
        "provider": NotRequired[Optional[AbsProvider]],
        "context_db": NotRequired[Optional[Union[DBAbstractConnector, Dict]]],
        "context_clear": NotRequired[Optional[ClearFunction]],
        "services": List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup]],
        "wrappers": NotRequired[Optional[List[Wrapper]]],
    },
)


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
    services: List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup]] = None
    wrappers: Optional[List[Wrapper]] = None

    timeout: int = -1

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.context_db = dict() if self.context_db is None else self.context_db
        self.wrappers = [] if self.wrappers is None else self.wrappers

        self.services = ServiceGroup.cast(
            self.services, self.actor, dict(), wrappers=self.wrappers, timeout=self.timeout
        )
        self._runner = Runner(self.actor, self.context_clear, self.context_db, self.provider, self.services)

    @property
    def flat_services_list(self) -> List[Tuple[str, Service]]:
        """
        Returns a copy of created inner services flat array used during actual pipeline running.
        Might be used for debugging purposes.
        """

        def get_flat_services_list(group: ServiceGroup, prefix: str) -> List[Tuple[str, Service]]:
            services = list()
            prefix = f"{prefix}.{group.name}"
            for service in group.annotators:
                if isinstance(service, Service):
                    services += [(prefix, service)]
                else:
                    services += get_flat_services_list(service, prefix)
            return services

        return get_flat_services_list(self.services, "")

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
        post_annotators: Optional[List[AnnotatorFunction]] = None,
    ):
        actor = Actor(script, start_label, fallback_label)
        context_db = dict() if context_db is None else context_db
        pre_annotators = [] if pre_annotators is None else pre_annotators
        post_annotators = [] if post_annotators is None else post_annotators
        return cls(
            actor=actor,
            provider=request_provider,
            context_db=context_db,
            services=pre_annotators + [ACTOR] + post_annotators,
        )

    @classmethod
    def from_dict(cls, dictionary: PipelineDict) -> "Pipeline":
        if "actor" not in dictionary:
            dictionary["actor"] = [serv for serv in dictionary["services"] if isinstance(serv, Actor)][0]
            dictionary["services"] = [ACTOR if isinstance(serv, Actor) else serv for serv in dictionary["services"]]
        return cls.parse_obj(dictionary)

    def __call__(self, request, ctx_id=uuid.uuid4()) -> Context:
        return run(self._runner._request_handler(request, ctx_id))

    async def async_call(self, request, ctx_id=uuid.uuid4()) -> Context:
        return await self._runner._request_handler(request, ctx_id)
