from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Wrapper, Pipeline, ServiceGroup, ACTOR
from df_runner.service import wrap
from examples import basic_example

actor = Actor(basic_example.SCRIPT, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))


@wrap(
    Wrapper(
        pre_func=lambda ctx, act, _: print("\t\tpre-wrapper"),
        post_func=lambda ctx, act, _: print("\t\tpost-wrapper"),
    )
)
def wrapped_service(ctx: Context, actor: Actor) -> Any:
    print(f"\t\t\tthe Service, that was wrapped")


class ActorWrapper(Wrapper):
    def __init__(self, **kwargs):
        def pre_func(ctx: Context, actor: Actor, _: str) -> Any:
            print(f"\t\tactor pre wrapper")
            last_request = str(ctx.last_request)
            ctx.requests[list(ctx.requests)[-1]] = last_request[:1].lower() + last_request[1:]

        super().__init__(pre_func=pre_func, post_func=lambda ctx, act, _: None, **kwargs)


pipeline = {
    "actor": actor,
    "provider": CLIProvider(),
    "connector": dict(),
    "services": [
        ServiceGroup(wrappers=[ActorWrapper()], services=[ACTOR]),
        wrapped_service,
    ],
}


pipeline = Pipeline(**pipeline)
if __name__ == "__main__":
    basic_example.test_pipeline(pipeline)
    pipeline.start_sync()