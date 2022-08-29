import asyncio
import logging
from typing import Any, Union, List, Dict, Optional

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script, Context
from df_engine.core.types import NodeLabel2Type

from ..service.service import Service
from ..service.wrapper import Wrapper
from ..message_interface import MessageInterface, CLIMessageInterface
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
        messaging interface (optionally) - an AbsMessagingInterface instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
        wrappers (optionally) - Wrapper classes array to add to all pipeline services
    """  # AFTER: upd description

    def __init__(
        self,
        message_interface: Optional[MessageInterface] = None,
        context_storage: Optional[Union[DBAbstractConnector, Dict]] = None,
        services: ServiceGroupBuilder = None,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
        optimization_warnings: bool = False,
    ):
        self.message_interface = CLIMessageInterface() if message_interface is None else message_interface
        self.context_storage = {} if context_storage is None else context_storage
        self._services_pipeline = ServiceGroup(
            services,
            wrappers=[] if wrappers is None else wrappers,
            timeout=timeout,
            name="pipeline",
        )
        self._services_pipeline.path = ".pipeline"
        self._services_pipeline = resolve_components_name_collisions(self._services_pipeline)

        if optimization_warnings:
            self._services_pipeline.log_optimization_warnings()

        flat_services = self._services_pipeline.get_subgroups_and_services()
        flat_services = [serv for _, serv in flat_services if isinstance(serv, Service)]
        actor = [serv.handler for serv in flat_services if isinstance(serv.handler, Actor)]
        self.actor = actor[0] if actor is not None and len(actor) > 0 else None
        if not isinstance(self.actor, Actor):
            raise Exception("Actor not found.")

    @property
    def info_dict(self) -> dict:
        return {
            "type": type(self).__name__,
            "message_interface": f"Instance of {type(self.message_interface).__name__}",
            "context_storage": f"Instance of {type(self.context_storage).__name__}",
            "services": [self._services_pipeline.info_dict],
        }

    def pretty_format(self, show_wrappers: bool = False, indent: int = 4) -> str:
        return print_component_info_dict(self.info_dict, show_wrappers, indent=indent)

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_storage: Optional[Union[DBAbstractConnector, Dict]] = None,
        message_interface: MessageInterface = CLIMessageInterface(),
        pre_services: Optional[List[ServiceBuilder]] = None,
        post_services: Optional[List[ServiceBuilder]] = None,
    ):
        actor = Actor(script, start_label, fallback_label)
        pre_services = [] if pre_services is None else pre_services
        post_services = [] if post_services is None else post_services
        return cls(
            message_interface=message_interface,
            context_storage=context_storage if context_storage is None else context_storage,
            services=[*pre_services, actor, *post_services],
        )

    @classmethod
    def from_dict(cls, dictionary: PipelineBuilder) -> "Pipeline":
        return cls(**dictionary)

    async def _run_pipeline(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_storage.get(ctx_id, Context(id=ctx_id))

        ctx.framework_states[PIPELINE_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self._services_pipeline(ctx, self.actor)
        del ctx.framework_states[PIPELINE_STATE_KEY]

        self.context_storage[ctx_id] = ctx
        return ctx

    def run(self):
        """
        Method for starting a pipeline, sets up corresponding messaging interface callback.
        Since one pipeline always has only one messaging interface, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """
        asyncio.run(self.message_interface.connect(self._run_pipeline))

    def __call__(self, request: Any, ctx_id: Any) -> Context:
        return asyncio.run(self._run_pipeline(request, ctx_id))
