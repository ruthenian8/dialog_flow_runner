import logging
from typing import Any, Union, List, Dict, TypedDict, Optional, Literal
import uuid
from asyncio import run

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script, Context
from df_engine.core.types import NodeLabel2Type
from pydantic import BaseModel, Extra
from typing_extensions import NotRequired

from .service_wrapper import Wrapper
from .provider import AbsProvider, CLIProvider
from .service_group import ServiceGroup
from .types import ServiceFunction, ClearFunction, ACTOR, AnnotatorFunction
from .service import Service
from .types import RUNNER_STATE_KEY


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
    Class that automates service execution and creates pipeline from dict and execution.
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

        self._pipeline = ServiceGroup.cast(
            self.services, self.actor, dict(), wrappers=self.wrappers, timeout=self.timeout
        )

    async def _run_pipeline(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_db.get(ctx_id)
        if ctx is None:
            ctx = Context()

        ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self._pipeline(ctx, self.actor)
        del ctx.framework_states[RUNNER_STATE_KEY]

        self.context_db[ctx_id] = ctx

        return ctx

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

    def start_sync(self) -> None:
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """

        def run_sync_run_pipeline(request: Any) -> Context:
            return run(self._run_pipeline(request, self.provider.ctx_id))

        self.provider.run(run_sync_run_pipeline)

    async def start_async(self) -> None:
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in sync context, asyncio.run() will produce error in async.
        """

        async def run_async_run_pipeline(request: Any) -> Context:
            return await self._run_pipeline(request, self.provider.ctx_id)

        await self.provider.run(run_async_run_pipeline)

    def __call__(self, request, ctx_id=uuid.uuid4()) -> Context:
        return run(self._run_pipeline(request, ctx_id))

    async def async_call(self, request, ctx_id=uuid.uuid4()) -> Context:
        return await self._run_pipeline(request, ctx_id)
