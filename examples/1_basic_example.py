from df_engine.core import Context

from df_runner import Pipeline
from examples._utils import SCRIPT

"""
The following example shows basic usage of `df_runner` module, as an extension to `df_engine`.

Pipeline is an object, that automates Actor execution and context management.
`from_script` method can be used to create a pipeline of the most basic structure:
"preprocessors -> actor -> postprocessors" as well as to define `context_storage` and `message_interface`.
These parameters usage will be shown in examples 2, 3 and 6.

Here only required for Actor creating parameters are provided to pipeline.
`context_storage` will default to simple Python dict and `message_interface` will never be used.
pre- and postprocessors lists are empty.
Pipeline object can be called with user input as first argument and dialog id (any immutable object).
This call will return Context, its `last_response` property will be actors response.
"""


pipeline = Pipeline.from_script(
    SCRIPT,  # Actor script object, defined in `.utils` module.
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


if __name__ == "__main__":
    while True:
        ctx: Context = pipeline(input("Send request: "), 0)  # 0 will be current dialog (context) identification.
        print(ctx.last_response)
