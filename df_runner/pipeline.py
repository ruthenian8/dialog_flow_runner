import logging
from typing import Any, Union, List, Dict, Optional, Tuple, Awaitable
import collections

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script, Context
from df_engine.core.types import NodeLabel2Type

from .service_wrapper import Wrapper
from .provider import AbsProvider, CLIProvider
from .service_group import ServiceGroup
from .types import ServiceBuilder, ServiceGroupBuilder, PipelineBuilder
from .service import Service
from .types import RUNNER_STATE_KEY
from .utils import run_in_current_or_new_loop


logger = logging.getLogger(__name__)


def get_subgroups_and_services_from_service_group(
    service_group: ServiceGroup,
    prefix: str = "",
    recursion_level=99,
) -> List[Tuple[str, Service]]:
    """
    Returns a copy of created inner services flat array used during actual pipeline running.
    Breadth First Algorithm
    """
    prefix += f".{service_group.name}"
    services = []
    if recursion_level > 0:
        recursion_level -= 1
        services += [(prefix, service) for service in service_group.services]
        for service in service_group.services:
            if not isinstance(service, Service):
                services += get_subgroups_and_services_from_service_group(service, prefix, recursion_level)
    return services


def create_service_name(base_name, services: List[Union[Service, ServiceGroup]]):
    name_index = 0
    while f"{base_name}_{name_index}" in [serv.name for serv in services]:
        name_index += 1
    return f"{base_name}_{name_index}"


def rename_same_service_prefix(services_pipeline):
    name_scoup_level = 0
    checked_names = 0
    while True:
        name_scoup_level += 1
        flat_services = get_subgroups_and_services_from_service_group(
            services_pipeline, recursion_level=name_scoup_level
        )
        flat_services = flat_services[checked_names:]
        if not flat_services:
            break
        checked_names += len(flat_services)
        flat_services = [(f"{prefix}.{serv.name}", serv) for prefix, serv in flat_services]
        paths, services = list(zip(*flat_services))
        path_counter = collections.Counter(paths)

        if max(path_counter.values()) > 1:
            # rename procedure for same paths
            for path, service in flat_services:
                if path_counter[path] > 1:
                    service.name = create_service_name(service.name, services)
    return services_pipeline


class Pipeline:
    """
    Class that automates service execution and creates pipeline from dict and execution.
    It also allows actor and annotators wrapping with special services, which enables more control over execution.
    It accepts:
        services - a Service list for this pipeline, should include Actor
        provider (optionally) - an AbsProvider instance for this pipeline
        connector (optionally) - an DBAbstractConnector instance for this pipeline or a dict
        wrappers (optionally) - Wrapper classes array to add to all pipeline services
    """

    def __init__(
        self,
        provider: Optional[AbsProvider] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        services: ServiceGroupBuilder = None,
        wrappers: Optional[List[Wrapper]] = None,
        timeout: int = -1,
    ):
        self.provider = CLIProvider() if provider is None else provider
        self.context_db = {} if context_db is None else context_db
        self.services = services
        self.wrappers = [] if wrappers is None else wrappers
        self.timeout = timeout
        self.services_pipeline = ServiceGroup(
            self.services,
            wrappers=self.wrappers,
            timeout=self.timeout,
            name="pipeline",
        )
        self.services_pipeline = rename_same_service_prefix(self.services_pipeline)

        flat_services = get_subgroups_and_services_from_service_group(self.services_pipeline)
        flat_services = [serv for _, serv in flat_services if isinstance(serv, Service)]
        actor = [serv.service_handler for serv in flat_services if isinstance(serv.service_handler, Actor)]
        self.actor = actor and actor[0]
        if not isinstance(self.actor, Actor):
            raise Exception("Actor not found.")

    @property
    def flat_services(self):
        return get_subgroups_and_services_from_service_group(self.services_pipeline)

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        request_provider: AbsProvider = CLIProvider(),
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

        ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self.services_pipeline(ctx, self.actor)
        del ctx.framework_states[RUNNER_STATE_KEY]

        self.context_db[ctx_id] = ctx

        return ctx

    def run(self):
        """
        Method for starting a pipeline, sets up corresponding provider callback.
        Since one pipeline always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """
        run_in_current_or_new_loop(self.provider.run(self._run_pipeline))

    def __call__(self, request: Any, ctx_id: Any) -> Union[Context, Awaitable[Context]]:
        return run_in_current_or_new_loop(self._run_pipeline(request, ctx_id))
