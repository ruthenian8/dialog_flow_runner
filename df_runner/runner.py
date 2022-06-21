from abc import abstractmethod, ABC
from typing import Any, Optional, Union, Callable, List, Dict

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from df_runner import AnnotatorFunctionType, Service, AbsProvider, CLIProvider
from df_runner.connector import CLIConnector


class AbsRunner(ABC):
    def __init__(
        self,
        connector: DBAbstractConnector = CLIConnector(),
        provider: AbsProvider = CLIProvider(),
        *args,
        **kwargs,
    ):
        self._connector = connector
        self._provider = provider

    def start(self, *args, **kwargs) -> None:
        while True:
            request = self._provider.request()
            ctx = self._request_handler(self._provider.ctx_id, request)
            self._provider.response(ctx.last_response)

    @abstractmethod
    def _request_handler(
        self,
        ctx_id: Any,
        ctx_update: Optional[Union[Any, Callable]],
        init_ctx: Optional[Union[Context, Callable]] = None,
    ) -> Context:
        raise NotImplementedError


class Runner(AbsRunner):
    def __init__(
        self,
        actor: Actor,
        connector: DBAbstractConnector = CLIConnector(),
        provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[Union[AnnotatorFunctionType, Service]]] = None,
        post_annotators: Optional[List[Union[AnnotatorFunctionType, Service]]] = None,
        *args,
        **kwargs
    ):
        super().__init__(connector, provider, *args, **kwargs)
        self._actor: Actor = actor
        self._pre_annotators = [] if pre_annotators is None else pre_annotators
        self._post_annotators = [] if post_annotators is None else post_annotators

    def _request_handler(
        self,
        ctx_id: Any,
        ctx_update: Optional[Union[Any, Callable]],
        init_ctx: Optional[Union[Context, Callable]] = None,
    ) -> Context:
        ctx: Context = self._connector.get(ctx_id)
        if ctx is None:
            if init_ctx is None:
                ctx: Context = Context()
            else:
                ctx: Context = init_ctx() if callable(init_ctx) else init_ctx

        if callable(ctx_update):
            ctx = ctx_update(ctx)
        else:
            ctx.add_request(ctx_update)

        # pre_annotators
        for annotator in self._pre_annotators:
            ctx = annotator(ctx, self._actor)

        ctx = self._actor(ctx)

        # post_annotators
        for annotator in self._post_annotators:
            ctx = annotator(ctx, self._actor)

        self._connector[ctx_id] = ctx

        return ctx


class ScriptRunner(Runner):
    def __init__(
        self,
        script: Union[Script, Dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        connector: DBAbstractConnector = CLIConnector(),
        request_provider: AbsProvider = CLIProvider(),
        pre_annotators: Optional[List[AnnotatorFunctionType]] = None,
        post_annotators: Optional[List[AnnotatorFunctionType]] = None,
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
