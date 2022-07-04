import logging
from asyncio import run, wait_for, as_completed, TimeoutError as AsyncTimeoutError, Task
from typing import Any, Optional, Union, List, Dict

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from df_runner import Service, AbsProvider, CLIProvider, ServiceFunction, ServiceState
from .context import merge


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

    def start(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        """
        async def callback(request: Any) -> Context:
            return await self._request_handler(request, self.provider.ctx_id)
        run(self.provider.run(callback))

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
            ctx.framework_states['RUNNER'] = dict()

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
        pre_annotators: Optional[List[ServiceFunction]] = None,
        post_annotators: Optional[List[ServiceFunction]] = None,
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
        connector: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        post_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        grouping: Optional[Dict[str, List[str]]] = None,
        *args,
        **kwargs
    ):
        super().__init__(actor, connector, provider, pre_annotators, post_annotators, *args, **kwargs)
        self._grouping = dict() if grouping is None else grouping

    async def _run_annotators(
        self,
        ctx: Context,
        actor: Actor,
        annotators: List[Union[ServiceFunction, Service]],
        cancel_waiting: bool = False
    ) -> Context:
        """
        A method for async services list procession.
        It manages service execution result: runs synchronous methods synchronously and collects asynchronous services tasks into a list.
        For every unfinished task a callback is added, rerunning services that have not already been run in case of requirements satisfaction.
        After all possible services run or, marks all pending services as FAILED and finishes.
        If timeout is exceeded for a service, throws an exception.
        """
        running = dict()
        for annotator in annotators:
            if ctx.framework_states['RUNNER'].get(annotator.name, ServiceState.NOT_RUN).value < 2:
                service_result = annotator(ctx, actor)
                if isinstance(service_result, Task):
                    timeout = annotator.timeout if isinstance(annotator, Service) and annotator.timeout > -1 else None
                    running.update({service_result.get_name(): wait_for(service_result, timeout=timeout)})
                else:
                    ctx = service_result

        results = []
        for name, future in zip(running.keys(), as_completed(running.values())):
            try:
                result = await future
                results.append(await self._run_annotators(result, actor, annotators))
            except AsyncTimeoutError as _:
                logger.warning(f"Service {name} timed out!")
        ctx = merge(ctx, *results)

        if cancel_waiting:
            for annotator in annotators:
                if ctx.framework_states['RUNNER'].get(annotator.name) == ServiceState.PENDING:
                    ctx.framework_states['RUNNER'][annotator.name] = ServiceState.FAILED

        return ctx

    async def _request_handler(
        self,
        request: Any,
        ctx_id: Optional[Any] = None
    ) -> Context:
        ctx = self.contex_db.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states['SERVICES'] = self._grouping
            ctx.framework_states['RUNNER'] = dict()

        ctx.add_request(request)

        ctx = await self._run_annotators(ctx, self.actor, self.pre_annotators, True)
        ctx = self.actor(ctx)
        ctx = await self._run_annotators(ctx, self.actor, self.post_annotators, True)

        ctx.framework_states['RUNNER'] = dict()
        self.contex_db[ctx_id] = ctx
        return ctx
