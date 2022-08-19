from typing import Any

from df_engine.core import Context, Actor
from flask import Flask, request

from df_runner import Pipeline, Service, CallbackProvider, ACTOR
from examples import basic_example


app = Flask(__name__)
actor = Actor(
    basic_example.SCRIPT, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node")
)


async def preprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpreprocession Service")


def postprocess(ctx: Context, actor: Actor) -> Any:
    print(f"\tpostprocession Service")


provider = CallbackProvider()

pipeline = {
    "actor": actor,
    "provider": provider,
    "connector": {},
    "services": [
        {"service": preprocess},
        {"service": ACTOR, "name": "encapsulated-actor"},
        Service(
            service=postprocess,
            name="postprocess",
        ),
    ],
}


@app.route("/df_provider")
async def route():
    req = request.args.get("request")
    return await provider.on_request_async(req)


pipeline = Pipeline(**pipeline)

if __name__ == "__main__":
    pipeline.start_sync()
    app.run()
    # Navigate to http://127.0.0.1:5000/df_provider?request=Hi
