from typing import Tuple

from df_db_connector import db_connector
from df_engine.core import Context, Actor

from df_runner import CLIProvider, Service
from df_runner.service import service_successful_condition

# TODO: actor needed?...


def preprocess(ctx: Context, actor: Actor) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


def postprocess(ctx: Context, actor: Actor) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


def print_misc(ctx: Context, actor: Actor) -> Tuple[Context, bool]:
    print(f"{ctx.misc=}")
    return ctx, True


pipeline = {
    "provider": CLIProvider,
    "db": db_connector,
    "services": [
        {
            "service": preprocess,
            "timeout": 1000
        },
        "actor",
        print_misc,
        Service(
            postprocess,
            name="postprocess",
            timeout=2000,
            start_condition=service_successful_condition("preprocess")
        )
    ]
}
