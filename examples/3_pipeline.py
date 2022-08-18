from asyncio import sleep
from typing import Any

from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd

from df_runner import CLIProvider, Service, Pipeline, ACTOR, FrameworkKeys

script = {
    "greeting_flow": {
        "start_node": {
            RESPONSE: "",
            TRANSITIONS: {"node1": cnd.exact_match("hi")},
        },
        "node1": {
            RESPONSE: "Hi, how are you?",
            TRANSITIONS: {"node2": cnd.exact_match("i'm fine, how are you?")},
        },
        "node2": {
            RESPONSE: "Good. What do you want to talk about?",
            TRANSITIONS: {"node3": cnd.exact_match("let's talk about music.")},
        },
        "node3": {
            RESPONSE: "Sorry, I can not talk about music now.",
            TRANSITIONS: {"node4": cnd.exact_match("ok, goodbye.")},
        },
        "node4": {RESPONSE: "bye", TRANSITIONS: {"node1": cnd.exact_match("hi")}},
        "fallback_node": {
            RESPONSE: "Ooops",
            TRANSITIONS: {"node1": cnd.exact_match("hi")},
        },
    }
}

actor = Actor(script, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))


def preprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpreprocession Service running (defined as a dict)")
    last_request = str(ctx.last_request)
    ctx.requests[list(ctx.requests)[-1]] = last_request[:1].lower() + last_request[1:]


def postprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpostprocession Service (defined as a callable)")
    return ctx.last_response == "Ooops"


async def postpostprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tanother postprocession Service (defined as a dict)")
    await sleep(1)
    print(f"\t\tThanks for waiting!")
    if ctx.framework_states[FrameworkKeys.SERVICES]["func_postprocess_0"]:
        print(f"\t\tI'm sorry, but after certain calculations, we assure you that you appear to be in fallback node!")
    else:
        print(f"\t\tCongratulations, you are not in fallback node now!")


pipeline = {
    "actor": actor,
    "provider": CLIProvider(),
    "context_db": {},
    "services": [
        {
            "service": preprocess,
            "timeout": 10,
        },
        ACTOR,
        postprocess,
        Service(
            service=postpostprocess,
            name="postprocess",
        ),
    ],
}


if __name__ == "__main__":
    pipe = Pipeline.parse_dict(pipeline)
    print("It may be not easy to understand what service names were generated for the pipeline.")
    print(
        f"Use given code in that case to acquire names: {[service.name for path, service in pipe.processed_services]}"
    )
    pipe.start_sync()
