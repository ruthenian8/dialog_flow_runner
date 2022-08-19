from typing import Any

from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd

from df_runner import CLIProvider, Service, Wrapper, Pipeline, ServiceGroup, ACTOR
from df_runner.conditions import service_successful_condition
from df_runner.service import wrap

script = {
    "greeting_flow": {
        "start_node": {
            RESPONSE: "",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},
        },
        "node1": {
            RESPONSE: "Hi, how are you?",
            TRANSITIONS: {"node2": cnd.exact_match("i'm fine, how are you?")},
        },
        "node2": {
            RESPONSE: "Good. What do you want to talk about?",
            TRANSITIONS: {"node3": cnd.exact_match("Let's talk about music.")},
        },
        "node3": {
            RESPONSE: "Sorry, I can not talk about music now.",
            TRANSITIONS: {"node4": cnd.exact_match("Ok, goodbye.")},
        },
        "node4": {RESPONSE: "bye", TRANSITIONS: {"node1": cnd.exact_match("Hi")}},
        "fallback_node": {
            RESPONSE: "Ooops",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},
        },
    }
}

actor = Actor(script, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))


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


if __name__ == "__main__":
    Pipeline(**pipeline).start_sync()
