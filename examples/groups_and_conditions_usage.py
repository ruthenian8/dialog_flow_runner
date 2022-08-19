from random import randint
from asyncio import sleep, run
from typing import Any

from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd

from df_runner import CLIProvider, Service, Pipeline, ServiceGroup, ACTOR, FrameworkKeys
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


def preprocess(ctx: Context, actor: Actor) -> Any:
    gen = randint(0, 2)
    print(f"\tpreprocession Service, generated {gen}")
    return gen


async def postprocess(ctx: Context, actor: Actor) -> Any:
    pre0 = ctx.framework_states[FrameworkKeys.SERVICES]["func_preprocess_0"]
    pre1 = ctx.framework_states[FrameworkKeys.SERVICES]["func_preprocess_1"]
    print(f"\tpostprocession Service, will sleep for {pre0 + pre1}")
    await sleep(pre0 + pre1)


async def postpostprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tWow! The postprocess service slept successfully!")


pipeline = {
    "actor": actor,
    "provider": CLIProvider(),
    "context_db": {},
    "services": [
        [
            {
                "service": preprocess,
                "timeout": 1,
            },
            {
                "service": preprocess,
                "timeout": 1,
            },
        ],
        ACTOR,
        {
            "service": postprocess,
            "timeout": 2,
            "start_condition": service_successful_condition(name="group_1"),
        },
        ServiceGroup(
            name="async-group",
            timeout=4,
            services=[
                {
                    "service": postprocess,
                    "timeout": 3,
                    "start_condition": service_successful_condition(name="group_1"),
                },
                Service(
                    service=postpostprocess,
                    name="postprocess",
                    start_condition=service_successful_condition(name="func_postprocess_0"),
                ),
            ],
        ),
    ],
}


if __name__ == "__main__":
    pipe = Pipeline.parse_dict(pipeline)
    print("It may be not easy to understand what service belong to which group in pipeline.")
    print(
        f"Use given code in that case to acquire services with their full path: {[f'{path}.{service.name}' for path, service in pipe.processed_services]}"
    )
    run(pipe.start_sync())
