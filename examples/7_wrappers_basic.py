import asyncio
import json
import logging
import random
from datetime import datetime

from df_engine.core import Context, Actor

from df_runner import Wrapper, Pipeline, ServiceGroup, WrapperRuntimeInfo
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows wrappers possibilities and use cases.

Wrappers are additional function pairs (before-function and after-function) that can be added to any pipeline components (service and service groups).
Wrappers main purpose should be service and service groups statistics collection.
Wrappers can be attached to pipeline component using `wrappers` constructor parameter.

Wrappers can have `before` and `after` functions - that will be executed before and after pipeline component respectively.

Here 5 `heavy_service`s are run in single asynchronous service group.
Each of them sleeps for random amount of seconds (between 0 and 5).
To each of them (as well as to group) time measurement wrapper is attached, that writes execution time to `ctx.misc`.
In the end `ctx.misc` is logged to info channel.
"""


def collect_timestamp_before(ctx: Context, _, info: WrapperRuntimeInfo):
    ctx.misc.update({f"{info['component']['name']}": datetime.now()})


def collect_timestamp_after(ctx: Context, _, info: WrapperRuntimeInfo):
    ctx.misc.update({f"{info['component']['name']}": datetime.now() - ctx.misc[f"{info['component']['name']}"]})


actor = Actor(
    SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)

time_measure_wrapper = Wrapper(before=collect_timestamp_before, after=collect_timestamp_after)


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 5))


def logging_service(ctx: Context):
    logger.info(f"Context misc:\n{json.dumps(ctx.misc, indent=4, default=str)}")


pipeline_dict = {
    "components": [
        ServiceGroup(
            wrappers=[time_measure_wrapper],
            components=[
                {
                    "handler": heavy_service,
                    "wrappers": [time_measure_wrapper],
                },
                {
                    "handler": heavy_service,
                    "wrappers": [time_measure_wrapper],
                },
                {
                    "handler": heavy_service,
                    "wrappers": [time_measure_wrapper],
                },
                {
                    "handler": heavy_service,
                    "wrappers": [time_measure_wrapper],
                },
                {
                    "handler": heavy_service,
                    "wrappers": [time_measure_wrapper],
                },
            ],
        ),
        actor,
        logging_service,
    ],
}


pipeline = Pipeline(**pipeline_dict)

if __name__ == "__main__":
    pipeline.run()
