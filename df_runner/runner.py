import logging
from asyncio import run
from typing import Any, Optional, Union, List, Dict, Callable

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from .group import ServiceGroup
from .provider import AbsProvider, CLIProvider
from .service import Service
from .types import FrameworkKeys, AnnotatorFunction, ClearFunction, ServiceFunction


logger = logging.getLogger(__name__)


class Runner:
    """
    Class, representing a runner, that executes actor together with pre- and post_annotators.
    Provider and connector are shared through the execution.
    The services are executed sequentially with a shared context.
    """

    def __init__(
        self,
        actor: Union[Actor, Service],
        contex_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        post_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        *args,
        **kwargs
    ):
        self.contex_db = dict() if contex_db is None else contex_db
        self.provider = provider
        self.actor = actor
        self.pre_annotators = [] if pre_annotators is None else pre_annotators
        self.post_annotators = [] if post_annotators is None else post_annotators

    def start_sync(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """
        async def callback(request: Any) -> Context:
            return await self._request_handler(request, self.provider.ctx_id)
        run(self.provider.run(callback))

    async def start_async(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        Use this in sync context, asyncio.run() will produce error in async.
        """
        async def callback(request: Any) -> Context:
            return await self._request_handler(request, self.provider.ctx_id)
        await self.provider.run(callback)

    async def _request_handler(
        self,
        request: Any,
        ctx_id: Optional[Any] = None
    ) -> Context:
        """
        Method for handling user input request through actor and all the annotators.
        :user_input: - input, received from user.
        :ctx_id: - id of current user in self._connector database (if not the first input).
        """
        ctx = self.contex_db.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states[FrameworkKeys.RUNNER] = dict()

        ctx.add_request(request)

        for annotator in self.pre_annotators:
            ctx = annotator(ctx, self.actor)

        ctx = self.actor(ctx)

        for annotator in self.post_annotators:
            ctx = annotator(ctx, self.actor)

        self.contex_db[ctx_id] = ctx

        return ctx


class ScriptRunner(Runner):
    """
    A standalone runner for scripts.
    It automatically creates actor and is alternative to pipelines.
    """

    def __init__(
        self,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        connector: Optional[Union[DBAbstractConnector, Dict]] = None,
        request_provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[AnnotatorFunction]] = None,
        post_annotators: Optional[List[AnnotatorFunction]] = None,
        *args,
        **kwargs,
    ):
        super(ScriptRunner, self).__init__(
            Actor(script, start_label, fallback_label),
            connector,
            request_provider,
            pre_annotators,
            post_annotators,
            *args,
            **kwargs,
        )


class PipelineRunner(Runner):
    """
    A special Runner for use in Pipelines.
    """

    def __init__(
        self,
        actor: Union[Actor, Service],
        clear_function: Optional[ClearFunction] = None,
        connector: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        group: Optional[ServiceGroup] = None,
        callback: Optional[Callable[[str, FrameworkKeys, Any], None]] = lambda name, key, data: None,
        *args,
        **kwargs
    ):
        super().__init__(actor, connector, provider, *args, **kwargs)
        self._callback = callback
        self._clear_function = clear_function
        self._group = group

    async def _request_handler(
        self,
        request: Any,
        ctx_id: Optional[Any] = None
    ) -> Context:
        ctx = self.contex_db.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states[FrameworkKeys.RUNNER] = dict()
            ctx.framework_states[FrameworkKeys.SERVICES] = dict()
            ctx.framework_states[FrameworkKeys.SERVICES_META] = dict()

        ctx.add_request(request)

        ctx = await self._group(ctx, self._callback, self.actor)

        ctx.framework_states[FrameworkKeys.RUNNER] = dict()
        if self._clear_function is not None:
            services, meta = self._clear_function(ctx.framework_states[FrameworkKeys.SERVICES], ctx.framework_states[FrameworkKeys.SERVICES_META])
            ctx.framework_states[FrameworkKeys.SERVICES] = services
            ctx.framework_states[FrameworkKeys.SERVICES_META] = meta
        self.contex_db[ctx_id] = ctx
        return ctx
