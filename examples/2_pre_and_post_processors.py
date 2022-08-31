import logging

from df_engine.core import Context

from df_runner import Pipeline, CLIMessageInterface
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows more advanced usage of `df_runner` module, as an extension to `df_engine`.

When Pipeline is created with `from_script` method, additional pre- and postprocessors can be defined.
These can be any ServiceBuilder objects (defined in `types` module) - callables, objects or dicts.
They are being turned into special Service objects (see example №3), that will be run before or after Actor respectively.
These services can be used to access external APIs, annotate user input, etc.

Here a preprocessor ("ping") and a postprocessor ("pong") are added to pipeline.
They share data in `context.misc` - a common place for sharing data between services and actor.
"""


# Callable signature can be one of the following: [ctx], [ctx, actor] or [ctx, actor, info], where:
#     ctx: Context - context of the current dialog
#     actor: Actor - actor of the pipeline
#     info: dict - dictionary, containing information about current pipeline execution state (see example №4)
def ping_processor(ctx: Context):
    ctx.misc["ping"] = True


def pong_processor(ctx: Context):
    ping = ctx.misc.get("ping", False)
    ctx.misc["pong"] = ping


pipeline = Pipeline.from_script(
    SCRIPT,
    ("greeting_flow", "start_node"),
    ("greeting_flow", "fallback_node"),
    {},  # `context_storage` - a dictionary or a `DBAbstractConnector` instance, a place to store dialog contexts
    CLIMessageInterface(),  # `message_interface` - a message channel adapter, it's not used in this example
    [ping_processor],
    [pong_processor],
)


if __name__ == "__main__":
    while True:
        ctx: Context = pipeline(input("Send request: "), 0)
        print(f"Response: {ctx.last_response}")
        ping_pong = ctx.misc.get("ping", False) and ctx.misc.get("pong", False)
        print(f"Ping-pong exchange: {'completed' if ping_pong else 'failed'}.")
        logger.info(f"Context misc: {ctx.misc}")
