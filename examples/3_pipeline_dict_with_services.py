import logging
import urllib.request

from df_engine.core import Context, Actor

from df_runner import CLIMessageInterface, Service, Pipeline
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows pipeline creation from dict and most important pipeline components.

When Pipeline is created using `from_dict` method, pipeline should be defined as PipelineBuilder objects (defined in `types` module).
These objects are dictionaries of particular structure:
    `message_interface` - `MessageInterface` instance, used to connect to channel and transfer IO to user
    `context_storage` - place to store dialog contexts (dictionary or a `DBAbstractConnector` instance)
    `services` (required) - a ServiceGroupBuilder object, basically a list of ServiceBuilder or ServiceGroupBuilder objects, see example №4
    `wrappers` - a list of pipeline wrappers, see example №7
    `timeout` - pipeline timeout, see example №5
    `optimization_warnings` - whether pipeline asynchronous structure should be checked during initialization, see example №5

On pipeline execution services from `services` list are run without difference between pre- and postprocessors.
If Actor instance is not found among `services` pipeline creation fails. There can be only one actor in the pipeline.
ServiceBuilder object can be defined either with callable (see example №2) or with dict of following structure / object with following constructor arguments:
    `handler` (required) - ServiceBuilder, if handler is n object or a dict itself, it will be used instead of base ServiceBuilder
    `wrappers` - a list of service wrappers, see example №7
    `timeout` - service timeout, see example №5
    `asynchronous` - whether or not this service _should_ be asynchronous (keep in mind that not all services _can_ be asynchronous), see example №5
    `start_condition` - service start condition, see example №4
    `name` - custom defined name for the service (keep in mind that names in one ServiceGroup should be unique), see example №4

Not only Pipeline can be run using `__call__` method, for most cases `run` method should be used.
It starts pipeline asynchronously and connects to provided message interface.

Here pipeline contains of 4 services, defined in 4 different ways.
First two of them write sample feature detection data to `ctx.misc`, first uses a constant expression and second fetches from `example.com`.
Third one is Actor (it acts like a _special_ service here). Final service logs `ctx.misc` dict.
"""


actor = Actor(
    SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def prepreprocess(ctx: Context):
    logger.info(f"preprocession intent-detection Service running (defined as a dict)")
    ctx.misc["preprocess_detection"] = {
        ctx.last_request: "some_intent"
    }  # Similar syntax can be used to access service output dedicated to current pipeline rum


def preprocess(ctx: Context):
    logger.info(f"another preprocession web-based annotator Service (defined as a callable)")
    with urllib.request.urlopen("https://example.com/") as webpage:
        web_content = webpage.read().decode(webpage.headers.get_content_charset())
        ctx.misc["another_detection"] = {ctx.last_request: "online" if "Example Domain" in web_content else "offline"}


def postprocess(ctx: Context):
    logger.info(f"postprocession Service (defined as an object)")
    logger.info(f"resulting misc looks like: {ctx.misc}")


pipeline = {
    "message_interface": CLIMessageInterface(
        intro="Hi, this is a brand new Pipeline running!", prompt_request="Request: ", prompt_response="Response: "
    ),  # `CLIMessageInterface` has the following constructor parameters:
    #     `intro` - a string that will be displayed on connection to interface (on `pipeline.run`)
    #     `prompt_request` - a string that will be displayed before user input
    #     `prompt_response` - an output prefix string
    "context_storage": {},
    "services": [
        {
            "handler": prepreprocess,
            "name": "preprocessor",
        },
        preprocess,
        actor,
        Service(
            handler=postprocess,
            name="postprocessor",
        ),
    ],
}


if __name__ == "__main__":
    Pipeline.from_dict(pipeline).run()
