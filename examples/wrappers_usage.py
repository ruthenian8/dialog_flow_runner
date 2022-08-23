from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Wrapper, Pipeline, ServiceGroup
from df_runner.service import wrap
from examples import basic_example

actor = Actor(
    basic_example.SCRIPT, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node")
)


@wrap(
    Wrapper(
        pre_func=lambda ctx, act, _: print("\t\tpre-wrapper"),
        post_func=lambda ctx, act, _: print("\t\tpost-wrapper"),
    )
)
def wrapped_service(ctx: Context, actor: Actor) -> Any:
    print(f"\t\t\tthe Service, that was wrapped")


wrap1 = Wrapper(
    pre_func=lambda ctx, act, _: print("\t\tpre-wrapper1"),
    post_func=lambda ctx, act, _: print("\t\tpost-wrapper1"),
)


def wrapped_service1(ctx: Context, actor: Actor) -> Any:
    print(f"\t\t\tthe Service, that was wrapped")


class ActorWrapper(Wrapper):
    def __init__(self, **kwargs):
        def pre_func(ctx: Context, actor: Actor, _: str) -> Any:
            print(f"\t\tactor pre wrapper")
            print(f"\t\tlast request 1st letter was {'lower' if str(ctx.last_request)[0].islower() else 'upper'}case")

        super().__init__(pre_func=pre_func, post_func=lambda ctx, act, _: None, **kwargs)


pipeline = {
    "provider": CLIProvider(),
    "context_db": dict(),
    "services": [
        ServiceGroup(wrappers=[wrap1], services=[wrapped_service1]),
        ServiceGroup(wrappers=[ActorWrapper()], services=[actor]),
        wrapped_service,
    ],
}


pipeline = Pipeline(**pipeline)

if __name__ == "__main__":
    pipeline.start()
