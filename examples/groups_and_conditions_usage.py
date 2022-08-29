from asyncio import sleep
from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIMessageInterface, Service, Pipeline, ServiceGroup
from examples import basic_example


actor = Actor(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def preprocess(ctx: Context, actor: Actor) -> Any:
    step = ctx.misc.get("step", 0) + 1
    ctx.misc["step"] = step
    if step % 2:
        ctx.misc["ping"] = (ctx.misc.get("ping", 0) + 1) % 2
    else:
        ctx.misc["pong"] = (ctx.misc.get("pong", 0) + 1) % 2


async def postprocess(ctx: Context, actor: Actor) -> Any:
    ping = ctx.misc["ping"]
    ping = ctx.misc["pong"]
    print(f"\tpostprocession Service, will sleep for {ping + ping}")


async def postpostprocess(ctx: Context, actor: Actor, info: dict) -> Any:
    print(f"\tWow! The postprocess service slept successfully!")
    print(info)


pipeline = {
    "message_interface": CLIMessageInterface(),
    "context_storage": {},
    "optimization_warnings": True,
    "services": [
        [
            {
                "handler": preprocess,
                "timeout": 1,
            },
            {
                "handler": preprocess,
                "timeout": 1,
            },
        ],
        actor,
        {
            "handler": postprocess,
            "timeout": 1,
        },
        ServiceGroup(
            name="service_group",
            timeout=4,
            services=[
                {
                    "handler": postprocess,
                    "timeout": 3,
                },
                Service(
                    handler=postpostprocess,
                    name="postprocess",
                ),
            ],
        ),
    ],
}


pipeline = Pipeline.from_dict(pipeline)

if __name__ == "__main__":
    print(pipeline.pretty_format())
    pipeline.run()
