from asyncio import sleep
from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Service, Pipeline, ServiceGroup
from examples import basic_example


actor = Actor(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def preprocess(ctx: Context, actor: Actor) -> Any:
    print("pink")
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
    await sleep(ping + ping)


async def postpostprocess(ctx: Context, actor: Actor, info: dict) -> Any:
    print(f"\tWow! The postprocess service slept successfully!")
    print(info)


pipeline = {
    "provider": CLIProvider(),
    "context_db": {},
    "optimization_warnings": True,
    "services": [
        [
            {
                "service_handler": preprocess,
                "timeout": 1,
            },
            {
                "service_handler": preprocess,
                "timeout": 1,
            },
        ],
        actor,
        {
            "service_handler": postprocess,
            "timeout": 1,
        },
        ServiceGroup(
            name="service_group",
            timeout=4,
            services=[
                {
                    "service_handler": postprocess,
                    "timeout": 3,
                },
                Service(
                    service_handler=postpostprocess,
                    name="postprocess",
                ),
            ],
        ),
    ],
}


pipeline = Pipeline.from_dict(pipeline)

if __name__ == "__main__":
    print(pipeline.to_string())
    pipeline.run()
