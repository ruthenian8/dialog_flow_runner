from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd

from df_runner import CLIProvider, Service, wrap, Wrapper, Pipeline, ServiceGroup
from df_runner.conditions import service_successful_condition

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


def preprocess(ctx: Context, actor: Actor) -> Context:
    print(f"    preprocession Service")
    return ctx


def postprocess(ctx: Context, actor: Actor) -> Context:
    print(f"    postprocession Service")
    return ctx


@wrap(Wrapper(pre_func=lambda ctx, act: print("        pre-wrapper"), post_func=lambda ctx, act: print("        post-wrapper")))
def wrapped_service(ctx: Context, actor: Actor) -> Context:
    print(f"            the Service, that was wrapped")
    return ctx


class ActorWrapper(Wrapper):
    def __init__(self, **kwargs):
        def pre_func(ctx: Context, actor: Actor) -> Context:
            print(f"        actor pre wrapper")
            return ctx

        def post_func(ctx: Context, actor: Actor) -> Context:
            print(f"        actor post wrapper")
            return ctx

        super().__init__(pre_func=pre_func, post_func=post_func, **kwargs)



pipeline = {
    "provider": CLIProvider(),
    "connector": dict(),
    "services": [
        {
            "service": preprocess,
            "timeout": 1000
        },
        ServiceGroup(
            wrappers=[ActorWrapper],
            services=[
                actor
            ]
        ),
        wrapped_service,
        Service(
            service=postprocess,
            name="postprocess",
            timeout=2000,
            start_condition=service_successful_condition("func_preprocess_0")
        )
    ]
}


if __name__ == "__main__":
    Pipeline(**pipeline).start_sync()
