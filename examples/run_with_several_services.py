from asyncio import sleep
from typing import Any

from df_engine.core import Context, Actor

from df_runner import CLIProvider, Service, Pipeline, ACTOR, FrameworkKeys
from examples import basic_example

actor = Actor(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


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
            "timeout": 3,
        },
        ACTOR,
        postprocess,
        Service(
            service=postpostprocess,
            name="postprocess",
        ),
    ],
}


pipeline = Pipeline.parse_dict(pipeline)
if __name__ == "__main__":
    basic_example.test_pipeline(pipeline)
    print("It may be not easy to understand what service names were generated for the pipeline.")
    print(
        "Use given code in that case to acquire "
        f"names: {[service.name for path, service in pipeline.processed_services]}"
    )
    pipeline.start_sync()
