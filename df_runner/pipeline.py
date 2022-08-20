import logging
from typing import Any, Union, List, Dict, TypedDict, Optional
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
from .types import ServiceFunction, AnnotatorFunction
from .service import Service
from .types import RUNNER_STATE_KEY
from .utils import get_flat_services_list


logger = logging.getLogger(__name__)

_ServiceCallable = Union[Service, ServiceFunction]

PipelineDict = TypedDict(
    "PipelineDict",
    {
        "provider": NotRequired[Optional[AbsProvider]],
        "context_db": NotRequired[Optional[Union[DBAbstractConnector, Dict]]],
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

        self._pipeline = ServiceGroup.cast(self.services, dict(), wrappers=self.wrappers, timeout=self.timeout)

        flat_services = get_flat_services_list(self._pipeline)
        actor = [serv.service for _, serv in flat_services if isinstance(serv.service, Actor)]
        self.actor = actor and actor[0]
        if not isinstance(self.actor, Actor):
            raise Exception("Actor not found.")

    async def _async_run_pipeline(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_db.get(ctx_id)
        if ctx is None:
            ctx = Context()

        ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self._pipeline(ctx, self.actor)
        del ctx.framework_states[RUNNER_STATE_KEY]

        self.context_db[ctx_id] = ctx

        return ctx

    async def _async_run_provider(self) -> Context:
        # callback for pipeline with provided ctx id
        async def run_with_ctx_pipeline(request: Any) -> Context:
            return await self._async_run_pipeline(request, self.provider.ctx_id)

        return await self.provider.run(run_with_ctx_pipeline)

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = {},
        request_provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[AnnotatorFunction]] = [],
        post_annotators: Optional[List[AnnotatorFunction]] = [],
    ):
        actor = Actor(script, start_label, fallback_label)
        return cls(
            provider=request_provider,
            context_db=context_db,
            services=pre_annotators + [actor] + post_annotators,
        )

    @classmethod
    def from_dict(cls, dictionary: PipelineDict) -> "Pipeline":
        return cls.parse_obj(dictionary)

    def start_sync(self) -> None:
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """
        run(self._async_run_provider())

    async def start_async(self) -> None:
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in sync context, asyncio.run() will produce error in async.
        """
        await self._async_run_provider()

    def __call__(self, request, ctx_id=uuid.uuid4()) -> Context:
        return run(self._async_run_pipeline(request, ctx_id))

    async def async_call(self, request, ctx_id=uuid.uuid4()) -> Context:
        return await self._async_run_pipeline(request, ctx_id)
