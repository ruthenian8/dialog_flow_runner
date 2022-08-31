import asyncio
import json
import logging
import random
from datetime import datetime

from df_engine.core import Actor

from df_runner import Pipeline, ServiceRuntimeInfo, ComponentExecutionState, CallbackType
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows how pipeline can be extended by callbacks and custom functions.

Pipeline functionality can be extended by callbacks.
Callbacks are special functions that are called on some stages of pipeline execution.
There are 4 types of callbacks:
    `BEFORE_ALL` - is called before pipeline execution
    `BEFORE` - is called before each service and service group execution
    `AFTER` - is called after each service and service group execution
    `AFTER_ALL` - is called after pipeline execution
Callbacks accept a single argument - ServiceRuntimeInfo, info about the service they are attached to and return None
Actually `BEFORE_ALL` and `AFTER_ALL` are attached to root service group named 'pipeline', so they return its runtime info

Callbacks are run via wrappers, so all the wrappers warnings (see example №7) are applicable to them.
Pipeline `add_callback` function is used to register all callbacks. It accepts following arguments:
    `callback_type` (required) - a CallbackType instance, indicates callback type to add
    `callback` (required) - the callback function itself
    `whitelist` - an optional list of paths, if it's not None the callback will be applied to specified pipeline components only
    `blacklist` - an optional list of paths, if it's not None the callback will be applied to all pipeline components except specified

Here basic functionality of `df-node-stats` library is emulated.
Information about pipeline component execution time and result is collected and printed to info log after pipeline execution.
Pipeline consists of actor and 25 `long_service`s that run random amount of time between 0 and 5 seconds.
"""


actor = Actor(
    SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)

start_times = dict()
pipeline_info = dict()


def before_all(info: ServiceRuntimeInfo):
    global start_times, pipeline_info
    now = datetime.now()
    pipeline_info = {'start_time': now}
    start_times = {info['path']: now}


def before(info: ServiceRuntimeInfo):
    start_times.update({info['path']: datetime.now()})


def after(info: ServiceRuntimeInfo):
    start_time = start_times[info['path']]
    pipeline_info.update({
        f"{info['path']}_duration": datetime.now() - start_time,
        f"{info['path']}_success": info['execution_state'].get(info['path'], ComponentExecutionState.NOT_RUN.name)
    })


def after_all(info: ServiceRuntimeInfo):
    pipeline_info.update({f'total_time': datetime.now() - start_times[info['path']]})
    logger.info(f"Pipeline stats:\n{json.dumps(pipeline_info, indent=4, default=str)}")


async def long_service(_, __, info: ServiceRuntimeInfo):
    timeout = random.randint(0, 5)
    logger.info(f"Service {info['name']} is going to sleep for {timeout} seconds.")
    await asyncio.sleep(timeout)


pipeline_dict = {
    "services": [
        [long_service for _ in range(0, 25)],
        actor,
    ],
}


pipeline = Pipeline(**pipeline_dict)

pipeline.add_callback(CallbackType.BEFORE_ALL, before_all)
pipeline.add_callback(CallbackType.BEFORE, before)
pipeline.add_callback(CallbackType.AFTER, after)
pipeline.add_callback(CallbackType.AFTER_ALL, after_all)

if __name__ == "__main__":
    pipeline.run()
