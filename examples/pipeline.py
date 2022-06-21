from typing import Tuple

from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd

from df_runner import CLIProvider, Service
from df_runner.connector import DefaultConnector
from df_runner.pipeline import ServiceDict, Pipeline
from df_runner.service import service_successful_condition

script = {
    "greeting_flow": {
        "start_node": {  # This is an initial node, it doesn't need an `RESPONSE`
            RESPONSE: "",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},  # If "Hi" == request of user then we make the transition
        },
        "node1": {
            RESPONSE: "Hi, how are you?",  # When the agent goes to node1, we return "Hi, how are you?"
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
        "fallback_node": {  # We get to this node if an error occurred while the agent was running
            RESPONSE: "Ooops",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},
        },
    }
}

actor = Actor(script, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))


def preprocess(ctx: Context) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


def postprocess(ctx: Context) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


def print_misc(ctx: Context) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


pipeline: ServiceDict = {
    "provider": CLIProvider,
    "connector": DefaultConnector,
    "services": [
        {
            "service": preprocess,
            "timeout": 1000
        },
        actor,
        print_misc,
        Service(
            postprocess,
            name="postprocess",
            timeout=2000,
            start_condition=service_successful_condition("preprocess")
        )
    ]
}


if __name__ == "__main__":
    for pipe in Pipeline.create(pipeline):
        pipe.run()
