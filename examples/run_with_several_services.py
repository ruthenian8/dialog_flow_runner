from asyncio import sleep
from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Service, Pipeline, get_subgroups_and_services_from_service_group
from examples import basic_example

actor = Actor(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def preprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpreprocession Service running (defined as a dict)")
    print(f"\t\tlast request 1st letter was {'lower' if str(ctx.last_request)[0].islower() else 'upper'}case")


def postprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpostprocession Service (defined as a callable)")
    ctx.misc["is_oops"] = ctx.last_response == "Ooops"


async def postpostprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tanother postprocession Service (defined as a dict)")
    await sleep(1)
    print(f"\t\tThanks for waiting!")
    if ctx.misc["is_oops"]:
        print(f"\t\tI'm sorry, but after certain calculations, we assure you that you appear to be in fallback node!")
    else:
        print(f"\t\tCongratulations, you are not in fallback node now!")


pipeline = {
    "provider": CLIProvider(),
    "context_db": {},
    "services": [
        {
            "service_handler": preprocess,
            "timeout": 3,
        },
        actor,
        postprocess,
        Service(
            service_handler=postpostprocess,
            name="postprocess",
        ),
    ],
}


pipeline = Pipeline.from_dict(pipeline)
if __name__ == "__main__":
    print("It may be not easy to understand what service names were generated for the pipeline.")
    print(
        "Use given code in that case to acquire "
        f"names: {[(prefix, service.name) for prefix, service in get_subgroups_and_services_from_service_group(pipeline.services_pipeline)]}"
    )
    pipeline.run()
