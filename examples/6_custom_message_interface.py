import logging

from df_engine.core import Context, Actor
from df_engine.core.context import get_last_index
from flask import Flask, request

from df_runner import Pipeline, CallbackMessageInterface, not_condition
from examples._utils import SCRIPT

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

"""
The following example shows message interfaces usage.

Message interfaces are used for providing a way for communication between user and pipeline.
They manage message channel initialization and termination as well as pipeline execution on every user request.
There are two built-in message interface types (that may be overridden):
    `PollingMessageInterface` - starts polling for user request in a loop upon initialization, it has following methods:
        `_request()` - method that is executed in a loop, should return list of tuples: (user request, unique dialog id)
        `_respond(responses)` - method that is executed in a loop after all user requests processing, accepts list of dialog Contexts
        `_on_exception(e)` - method that is called on exception that happens in the loop, should catch the exception (it is also called on pipeline termination)
        `connect(pipeline_runner, loop, timeout)` - method that is called on connection to message channel, accepts pipeline_runner (a callback, running pipeline)
            loop - a function to be called on each loop execution (should return True to continue polling)
            timeout - time in seconds to wait between loop executions
    `CallbackMessageInterface` - creates message channel and provides a callback for pipeline execution, it has following methods:
        `on_request(request, ctx_id)` - method that should be called each time user provides new input to pipeline, returns dialog Context
`CLIMessageInterface` is also a message interface that overrides `PollingMessageInterface` and provides default message channel between pipeline and console/file IO

Here a default `CallbackMessageInterface` is used to setup communication between pipeline and Flask server.
Two services are used to process request:
    `purify_request` extracts user request from Flask HTTP request
    `construct_webpage_by_response` wraps actor response in webpage and adds response-based image to it
"""

app = Flask(__name__)

actor = Actor(SCRIPT, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))

message_interface = (
    CallbackMessageInterface()
)  # For this simple case of Flask, CallbackMessageInterface may not be overridden


def construct_webpage_by_response(response: str):
    return f"""
    <!DOCTYPE html>
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                p {{text-align: center;}}
                img {{
                    display: block;
                    margin-left: auto;
                    margin-right: auto;
                }}
            </style>
        </head>
        <body>
            <p><b>{response}</b></p>
            <img src="https://source.unsplash.com/random?{response}" alt="Response picture" style="width:50%;height:50%;">
        </body>
    </html>
    """


def purify_request(ctx: Context):
    last_request = ctx.last_request  # TODO: add _really_ nice ways to modify user request and response
    logger.info(f"Capturing request from: {last_request.base_url}")
    last_index = get_last_index(ctx.requests)
    ctx.requests[last_index] = last_request.args.get("request")


def markdown_request(ctx: Context):
    last_response = ctx.last_response
    last_index = get_last_index(ctx.responses)
    ctx.responses[last_index] = construct_webpage_by_response(last_response)


running_tests = True  # Protects request from being extracted and response from being wrapped into HTML when pipeline is used with tests

pipeline_dict = {
    "message_interface": message_interface,
    "services": [
        {
            "handler": purify_request,
            "start_condition": not_condition(lambda _, __: running_tests),  # Runs only if not running_tests
        },
        {
            "handler": actor,
            "name": "encapsulated-actor",
        },  # Actor here is encapsulated in another service with specific name
        {
            "handler": markdown_request,
            "start_condition": not_condition(lambda _, __: running_tests),  # Runs only if not running_tests
        },
    ],
}


@app.route("/pipeline_web_interface")
async def route():
    return message_interface.on_request(request, 0).last_response


pipeline = Pipeline(**pipeline_dict)

if __name__ == "__main__":
    running_tests = False  # TODO: add pipeline-wide arguments storage available to services amd conditions
    pipeline.run()
    app.run()
    # Navigate to http://127.0.0.1:5000/pipeline_web_interface?request={REQUEST} to receive response
    # e.g. http://127.0.0.1:5000/pipeline_web_interface?request=Hi will bring you to actor start node
