import logging

from df_engine.core import Actor

from df_runner import Service, Pipeline
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows pipeline creation from dict and most important pipeline components.

When Pipeline is created using `from_dict` method, pipeline should be defined as dictionary.
It should contain `services` - a ServiceGroupBuilder object, basically a list of ServiceBuilder or ServiceGroupBuilder objects, see example №4.

On pipeline execution services from `services` list are run without difference between pre- and postprocessors.
Actor instance should also be present among services.
ServiceBuilder object can be defined either with callable (see example №2) or with dict / object.
It should contain `handler` - a ServiceBuilder object.

Not only Pipeline can be run using `__call__` method, for most cases `run` method should be used.
It starts pipeline asynchronously and connects to provided messenger interface.

Here pipeline contains 4 services, defined in 4 different ways with different signatures.
"""


actor = Actor(
    SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


def prepreprocess(_):
    logger.info(f"preprocession intent-detection Service running (defined as a dict)")


def preprocess(_):
    logger.info(f"another preprocession web-based annotator Service (defined as a callable)")


def postprocess(_):
    logger.info(f"postprocession Service (defined as an object)")


pipeline_dict = {
    "components": [
        {
            "handler": prepreprocess,
        },
        preprocess,
        actor,
        Service(
            handler=postprocess,
        ),
    ],
}


pipeline = Pipeline.from_dict(pipeline_dict)

if __name__ == "__main__":
    pipeline.run()
