from abc import ABC, abstractmethod
from typing import Any, Optional, Union, Callable

from df_engine.core import Context, Actor, Script
from df_engine.core.types import NodeLabel2Type
from df_db_connector import DBAbstractConnector

from df_runner import AbsProvider, CLIProvider


class AbsRunner(ABC):
    @abstractmethod
    def start(self, *args, **kwargs) -> None:
        raise NotImplementedError


class Runner(AbsRunner):
    def __init__(
        self,
        actor: Actor,
        db: Optional[DBAbstractConnector] = None,
        request_provider: Optional[AbsProvider] = None,
        pre_annotators: Optional[list] = None,
        post_annotators: Optional[list] = None,
        *args,
        **kwargs,
    ):
        self._db: DBAbstractConnector = dict() if db is None else db
        self._actor: Actor = actor
        self._request_provider: AbsProvider = CLIProvider() if request_provider is None else request_provider
        self._pre_annotators: list = [] if pre_annotators is None else pre_annotators
        self._post_annotators: list = [] if post_annotators is None else post_annotators

    def start(self, *args, **kwargs) -> None:
        while self._request_provider.run(self):
            pass

    def request_handler(
        self,
        ctx_id: Any,
        ctx_update: Optional[Union[Any, Callable]],
        init_ctx: Optional[Union[Context, Callable]] = None,
    ) -> Context:
        # db
        ctx: Context = self._db.get(ctx_id)
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

        self._db |= {ctx_id: ctx}

        return ctx


class ScriptRunner(Runner):
    def __init__(
        self,
        script: Union[Script, dict],
        start_label: NodeLabel2Type,
        fallback_label: Optional[NodeLabel2Type] = None,
        db: DBAbstractConnector = dict(),
        request_provider: AbsProvider = CLIProvider(),
        pre_annotators: list = [],
        post_annotators: list = [],
        *args,
        **kwargs,
    ):
        super(ScriptRunner, self).__init__(
            Actor(script, start_label, fallback_label),
            db,
            request_provider,
            pre_annotators,
            post_annotators,
            *args,
            **kwargs,
        )
