from typing import Any

from df_engine.core import Context, Actor
from flask import Flask, request

from df_runner import Pipeline, Service, CallbackMessageInterface
from examples import basic_example


app = Flask(__name__)
actor = Actor(
    basic_example.SCRIPT, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node")
)


async def preprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpreprocession Service")


def postprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpostprocession Service")


message_interface = CallbackMessageInterface()

pipeline = {
    "message_interface": message_interface,
    "context_storage": {},
    "services": [
        {"handler": preprocess},
        {"handler": actor, "name": "encapsulated-actor"},
        Service(
            handler=postprocess,
            name="postprocess",
        ),
    ],
}


@app.route("/df_provider")
async def route():
    req = request.args.get("request")
    return message_interface.on_request(req, 0).last_response


pipeline = Pipeline(**pipeline)

if __name__ == "__main__":
    pipeline.run()
    app.run()
    # Navigate to http://127.0.0.1:5000/df_provider?request=Hi
