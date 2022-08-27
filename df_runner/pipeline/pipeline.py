import logging
from asyncio import run  # TODO: replace by `import asyncio`  and use `asyncio.run` instead `run`
from typing import Any, Union, List, Dict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script, Context
from df_engine.core.types import NodeLabel2Type

from ..service.service import Service
from ..service.wrapper import Wrapper
from ..provider import AbsProvider, CLIProvider
from ..service.group import ServiceGroup
from ..types import ServiceBuilder, ServiceGroupBuilder, PipelineBuilder
from ..types import PIPELINE_STATE_KEY
from .utils import resolve_components_name_collisions, print_component_info_dict

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Class that automates service execution and creates pipeline from dict and execution.
    It also allows actor and annotators wrapping with special services, which enables more control over execution.
    It accepts:
        services - a Service list for this pipeline, should include Actor
        provider (optionally) - an AbsProvider instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
        wrappers (optionally) - Wrapper classes array to add to all pipeline services
    """ # TODO: upd description

    def __init__(
        self,
        provider: Optional[AbsProvider] = None,  # NAMING: connected to Provider naming
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,  # NAMING: context, but not always db
        services: ServiceGroupBuilder = None,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        optimization_warnings: bool = False,
    ):
        self.provider = CLIProvider() if provider is None else provider
        self.context_db = {} if context_db is None else context_db
        self._services_pipeline = ServiceGroup(
            services,
            wrappers=[] if wrappers is None else wrappers,
            timeout=timeout,
            name="pipeline",
        )
        self._services_pipeline = resolve_components_name_collisions(self._services_pipeline)

        if optimization_warnings:
            self._services_pipeline.log_optimization_warnings()

        flat_services = self._services_pipeline.get_subgroups_and_services()
        flat_services = [serv for _, serv in flat_services if isinstance(serv, Service)]
        actor = [serv.service_handler for serv in flat_services if isinstance(serv.service_handler, Actor)]
        self.actor = actor and actor[0]  # FIXME: obscure syntax
        if not isinstance(self.actor, Actor):
            raise Exception("Actor not found.")

    @property
    def info_dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "provider": f"Instance of {type(self.provider).__name__}",
            "context_db": f"Instance of {type(self.context_db).__name__}",
            "services": [self._services_pipeline.info_dict],
        }

    def to_string(self, show_wrappers: bool = False) -> str:  # NAMING: 'pretty_print', add pretty printing parameters (like offsets, etc.)
        return print_component_info_dict(self.info_dict, show_wrappers)

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,  # NAMING: connected to context_db
        request_provider: AbsProvider = CLIProvider(),  # NAMING: connected to Provider naming
        pre_services: Optional[List[ServiceBuilder]] = None,
        post_services: Optional[List[ServiceBuilder]] = None,
    ):
        actor = Actor(script, start_label, fallback_label)
        pre_services = [] if pre_services is None else pre_services
        post_services = [] if post_services is None else post_services
        return cls(
            provider=request_provider,
            context_db=context_db if context_db is None else context_db,
            services=[*pre_services, actor, *post_services],
        )

    @classmethod
    def from_dict(cls, dictionary: PipelineBuilder) -> "Pipeline":
        return cls(**dictionary)

    async def _run_pipeline(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_db.get(ctx_id, Context(id=ctx_id))

        ctx.framework_states[PIPELINE_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self._services_pipeline(ctx, self.actor)
        del ctx.framework_states[PIPELINE_STATE_KEY]

        self.context_db[ctx_id] = ctx
        return ctx

    def run(self):
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """
        run(self.provider.run(self._run_pipeline))

    def __call__(self, request: Any, ctx_id: Any) -> Context:
        return run(self._run_pipeline(request, ctx_id))
