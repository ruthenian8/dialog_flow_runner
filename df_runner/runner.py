import logging
from asyncio import run
from typing import Any, Optional, Union, Dict

from df_engine.core import Context, Actor
from df_db_connector import DBAbstractConnector

from .group import ServiceGroup
from .provider import AbsProvider, CLIProvider
from .service import Service
from .types import FrameworkKeys, ClearFunction

logger = logging.getLogger(__name__)


class Runner:
    """
    Class, representing a runner for use in Pipelines.
    Provider and connector are shared through the execution.
    The services are executed sequentially with a shared context.
    """

    def __init__(
        self,
        actor: Union[Actor, Service],
        clear_function: Optional[ClearFunction] = None,
        contex_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        group: Optional[ServiceGroup] = None,
    ):
        self.contex_db = dict() if contex_db is None else contex_db
        self.provider = provider
        self.actor = actor
        self._clear_function = clear_function
        self._group = group

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

    async def _request_handler(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.contex_db.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states[FrameworkKeys.RUNNER] = dict()
            ctx.framework_states[FrameworkKeys.SERVICES] = dict()
            ctx.framework_states[FrameworkKeys.SERVICES_META] = dict()

        ctx.add_request(request)
        ctx = await self._group(ctx, self.actor)

        ctx.framework_states[FrameworkKeys.RUNNER] = dict()
        if self._clear_function is not None:
            services, meta = self._clear_function(
                ctx.framework_states[FrameworkKeys.SERVICES], ctx.framework_states[FrameworkKeys.SERVICES_META]
            )
            ctx.framework_states[FrameworkKeys.SERVICES] = services
            ctx.framework_states[FrameworkKeys.SERVICES_META] = meta
        self.contex_db[ctx_id] = ctx

        return ctx
