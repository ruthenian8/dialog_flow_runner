import logging
from asyncio import run
from typing import Any, Optional, Union, Dict

from df_engine.core import Context, Actor
from df_db_connector import DBAbstractConnector

from .service_group import ServiceGroup
from .provider import AbsProvider, CLIProvider
from .service import Service
from .types import RUNNER_STATE_KEY, ClearFunction

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
        context_db: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        group: Optional[ServiceGroup] = None,
    ):
        self.context_db = dict() if context_db is None else context_db
        self.provider = provider
        self.actor = actor
        self._group = group

    def start_sync(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        Use this in async context, await will not work in sync.
        """

        def run_sync_request_handler(request: Any) -> Context:
            return run(self._request_handler(request, self.provider.ctx_id))

        self.provider.run(run_sync_request_handler)

    async def start_async(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        Use this in sync context, asyncio.run() will produce error in async.
        """

        async def run_async_request_handler(request: Any) -> Context:
            return await self._request_handler(request, self.provider.ctx_id)

        await self.provider.run(run_async_request_handler)

    async def _request_handler(self, request: Any, ctx_id: Optional[Any] = None) -> Context:
        ctx = self.context_db.get(ctx_id)
        if ctx is None:
            ctx = Context()

        ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.add_request(request)
        ctx = await self._group(ctx, self.actor)
        del ctx.framework_states[RUNNER_STATE_KEY]

        self.context_db[ctx_id] = ctx

        return ctx
