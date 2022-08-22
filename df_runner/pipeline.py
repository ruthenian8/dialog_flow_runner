import logging
from typing import Any, Union, List, Dict, TypedDict, Optional, Tuple
import uuid
from asyncio import run
import collections

from df_db_connector import DBAbstractConnector
from df_engine.core import Actor, Script, Context
from df_engine.core.types import NodeLabel2Type
from typing_extensions import NotRequired

from .service_wrapper import Wrapper
from .provider import AbsProvider, CLIProvider
from .service_group import ServiceGroup
from .types import Handler, AnnotatorFunction
from .service import Service
from .types import RUNNER_STATE_KEY


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


_ServiceCallable = Union[Service, Handler]

PipelineDict = TypedDict(
    "PipelineDict",
    {
        "provider": NotRequired[Optional[AbsProvider]],
        "context_db": NotRequired[Optional[Union[DBAbstractConnector, Dict]]],
        "services": List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup]],
        "wrappers": NotRequired[Optional[List[Wrapper]]],
    },
)


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
        provider: Optional[AbsProvider] = CLIProvider(),
        context_db: Optional[Union[DBAbstractConnector, Dict]] = {},
        services: List[Union[_ServiceCallable, List[_ServiceCallable], ServiceGroup]] = None,
        wrappers: Optional[List[Wrapper]] = [],
        timeout: int = -1,
        **kwargs,
    ):
        self.provider = provider
        self.context_db = context_db
        self.services = services
        self.wrappers = wrappers
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

    @classmethod
    def from_script(
        cls,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        context_db: Optional[Union[DBAbstractConnector, Dict]] = {},
        request_provider: AbsProvider = CLIProvider(),
        pre_services: Optional[List[AnnotatorFunction]] = [],
        post_services: Optional[List[AnnotatorFunction]] = [],
    ):
        actor = Actor(script, start_label, fallback_label)
        return cls(
            provider=request_provider,
            context_db=context_db,
            services=pre_services + [actor] + post_services,
        )

    @classmethod
    def from_dict(cls, dictionary: PipelineDict) -> "Pipeline":
        return cls(**dictionary)

    async def _async_run_pipeline(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_db.get(ctx_id, Context(id=ctx_id))

        ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self.services_pipeline(ctx, self.actor)
        del ctx.framework_states[RUNNER_STATE_KEY]

        self.context_db[ctx_id] = ctx

        return ctx

    async def _async_run_provider(self) -> Context:
        # create callback for pipeline with provided ctx id
        async def run_with_ctx_pipeline(request: Any) -> Context:
            return await self._async_run_pipeline(request, self.provider.ctx_id)

        return await self.provider.run(run_with_ctx_pipeline)

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
