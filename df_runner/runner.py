from asyncio import Future, run, Task, gather
from typing import Any, Optional, Union, List, Dict

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from df_runner import Service, AbsProvider, CLIProvider, ServiceFunction, ServiceState
from .context import merge


class Runner:
    """
    Class, representing a runner, that executes actor together with pre- and post_annotators.
    Provider and connector are shared through the execution.
    The services are executed sequentially with a shared context.
    """

    def __init__(
        self,
        actor: Union[Actor, Service],
        connector: Optional[Union[DBAbstractConnector, Dict]] = None,
        provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        post_annotators: Optional[List[Union[ServiceFunction, Service]]] = None,
        *args,
        **kwargs
    ):
        self._connector = dict() if connector is None else connector
        self._provider = provider
        self._actor = actor
        self._pre_annotators = [] if pre_annotators is None else pre_annotators
        self._post_annotators = [] if post_annotators is None else post_annotators

    def start(self) -> None:
        """
        Method for starting a runner, sets up corresponding provider callback.
        Since one runner always has only one provider, there is no need for thread management here.
        """
        def callback(request: Any) -> Context:
            return self._request_handler(request, self._provider.ctx_id)
        self._provider.run(callback)

    def _request_handler(
        self,
        request: Any,
        ctx_id: Optional[Any] = None
    ) -> Context:
        """
        Method for handling user input request through actor and all the annotators.
        :user_input: - input, received from user.
        :ctx_id: - id of current user in self._connector database (if not the first input).
        """
        ctx = self._connector.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states['RUNNER'] = dict()

        ctx.add_request(request)

        for annotator in self._pre_annotators:
            ctx = annotator(ctx, self._actor)

        ctx = self._actor(ctx)

        for annotator in self._post_annotators:
            ctx = annotator(ctx, self._actor)

        self._connector[ctx_id] = ctx

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
        async def rerun(task: Task):
            result = task.result()
            return await self._run_annotators(result, actor, annotators)

        running = set()
        for annotator in annotators:
            if ctx.framework_states['RUNNER'].get(annotator.name, ServiceState.NOT_RUN).value < 2:
                service_result = annotator(ctx, actor)
                if isinstance(service_result, Future):
                    service_result.add_done_callback(rerun)
                    running.add(service_result)
                else:
                    ctx = service_result

        if cancel_waiting:
            for annotator in annotators:
                if ctx.framework_states['RUNNER'].get(annotator.name) == ServiceState.PENDING:
                    ctx.framework_states['RUNNER'][annotator.name] = ServiceState.FAILED

        contexts = await gather(*running)
        return merge(ctx, *contexts)

    def _request_handler(
        self,
        request: Any,
        ctx_id: Optional[Any] = None
    ) -> Context:
        ctx = self._connector.get(ctx_id)
        if ctx is None:
            ctx = Context()
            ctx.framework_states['SERVICES'] = self._grouping
            ctx.framework_states['RUNNER'] = dict()

        ctx.add_request(request)

        ctx = run(self._run_annotators(ctx, self._actor, self._pre_annotators, True))
        ctx = self._actor(ctx)
        ctx = run(self._run_annotators(ctx, self._actor, self._post_annotators, True))

        ctx.framework_states['RUNNER'] = dict()
        self._connector[ctx_id] = ctx
        return ctx
