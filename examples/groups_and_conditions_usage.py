from random import randint
from asyncio import sleep, run
from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Service, Pipeline, ServiceGroup, ACTOR, FrameworkKeys
from df_runner.conditions import service_successful_condition
from examples import basic_example

actor = Actor(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def preprocess(ctx: Context, actor: Actor) -> Any:
    gen = randint(0, 2) / 2
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
            "timeout": 1,
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


pipeline = Pipeline.from_dict(pipeline)
if __name__ == "__main__":
    print("It may be not easy to understand what service belong to which group in pipeline.")
    print(
        "Use given code in that case to acquire services "
        f"with their full path: {[f'{path}.{service.name}' for path, service in pipeline.processed_services]}"
    )
    run(pipeline.start_sync())
