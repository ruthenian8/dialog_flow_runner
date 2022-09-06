import json
import logging
import random
from datetime import datetime

import psutil
from df_engine.core import Context, Actor

from df_runner import Wrapper, Pipeline, ServiceGroup, to_service, WrapperRuntimeInfo, ServiceRuntimeInfo
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows wrappers possibilities and use cases.

Wrappers are additional function pairs (before-function and after-function) that can be added to any pipeline components (service and service groups).
Despite wrappers can be used to prepare data for certain services, that require some very special input type, in most cases services should be preferred for that purpose.
Wrappers can't be asynchronous, there's also no statistics that can be collected about them.
So their main purpose should be _really_ lightweight data conversion (etc.) operations or service and service groups statistics collection.

Wrappers have the following constructor arguments / parameters:
    `before` - function that will be run before wrapped component
    `after` - function that will be run after wrapped component
    `name` - wrapper name
NB! Wrappers don't have execution state, so their names shouldn't appear in built-in condition functions

Wrapper callable signature can be one of the following: [ctx], [ctx, actor] or [ctx, actor, info], where:
    `ctx` - Context of the current dialog
    `actor` - Actor of the pipeline
    `info` - dictionary, containing information about current wrapper and pipeline execution state (see example №4)

Wrappers can be attached to pipeline component in few different ways:
    1. Directly in constructor - by adding wrappers to `wrappers` constructor parameter
    2. (Services only) `with_wrappers` decorator - transforms function to service with wrappers from `*wrappers` argument
    3. (Services only) `wrap_with` decorator - transforms function to service with wrapper, created from `before`, `after` and `name` arguments

Here 5 `heavy_service`s fill big amounts of memory with random numbers.
Their runtime stats are captured and displayed by wrappers, `time_measure_wrapper` measures time and `ram_measure_wrapper` - allocated memory.
Another `time_measure_wrapper` measures total amount of time taken by all of them (combined in service group).
`logging_service` logs stats, however it can use string arguments only, so `json_encoder_wrapper` is applied to encode stats to JSON.
"""


def get_wrapper_misc_field(
    info: WrapperRuntimeInfo, postfix: str
) -> str:  # This method calculates `misc` field name dedicated to wrapper based on its and its service name
    return f"{info['component']['name']}-{postfix}"


actor = Actor(
    SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)

memory_heap = dict()  # This object plays part of some memory heap

time_measure_wrapper = Wrapper(
    before=lambda ctx, _, info: ctx.misc.update({get_wrapper_misc_field(info, "time"): datetime.now()}),
    after=lambda ctx, _, info: ctx.misc.update(
        {get_wrapper_misc_field(info, "time"): datetime.now() - ctx.misc[get_wrapper_misc_field(info, "time")]}
    ),
    name="time_measure_wrapper",
)

ram_measure_wrapper = Wrapper(
    before=lambda ctx, _, info: ctx.misc.update(
        {get_wrapper_misc_field(info, "ram"): psutil.virtual_memory().available}
    ),
    after=lambda ctx, _, info: ctx.misc.update(
        {
            get_wrapper_misc_field(info, "ram"): ctx.misc[get_wrapper_misc_field(info, "ram")]
            - psutil.virtual_memory().available
        }
    ),
    name="ram_measure_wrapper",
)


json_convertor_service = Wrapper(
    before=lambda ctx, _, info: ctx.misc.update(
        {get_wrapper_misc_field(info, "str"): json.dumps(ctx.misc, indent=4, default=str)}
    ),
    after=lambda ctx, _, info: ctx.misc.pop(get_wrapper_misc_field(info, "str")),
    name="json_converter",
)


@to_service(wrappers=[time_measure_wrapper, ram_measure_wrapper])
def heavy_service(ctx: Context):
    memory_heap[ctx.last_request] = [random.randint(0, num) for num in range(0, 100000)]


@to_service(wrappers=[json_convertor_service])
def logging_service(ctx: Context, _, info: ServiceRuntimeInfo):
    str_misc = ctx.misc[f"{info['name']}-str"]
    assert isinstance(str_misc, str)
    logger.info(f"Stringified misc:\n{str_misc}")


pipeline_dict = {
    "components": [
        ServiceGroup(wrappers=[time_measure_wrapper], components=[heavy_service for _ in range(0, 5)]),
        actor,
        logging_service,
    ],
}


pipeline = Pipeline(**pipeline_dict)

if __name__ == "__main__":
    pipeline.run()
