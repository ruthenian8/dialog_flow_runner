from typing import Any, Optional, Union, List, Dict

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from df_runner import Service, AbsProvider, CLIProvider, ServiceFunction


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
        self._actor: Actor = actor
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
