from df_engine.core import Context, Actor
from df_engine.core.keywords import RESPONSE, TRANSITIONS
import df_engine.conditions as cnd
from flask import Flask, request

from df_runner import Pipeline, Service, CallbackProvider
from df_runner.conditions import service_successful_condition


app = Flask(__name__)

script = {
    "greeting_flow": {
        "start_node": {
            RESPONSE: "",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},
        },
        "node1": {
            RESPONSE: "Hi, how are you?",
            TRANSITIONS: {"node2": cnd.exact_match("i'm fine, how are you?")},
        },
        "node2": {
            RESPONSE: "Good. What do you want to talk about?",
            TRANSITIONS: {"node3": cnd.exact_match("Let's talk about music.")},
        },
        "node3": {
            RESPONSE: "Sorry, I can not talk about music now.",
            TRANSITIONS: {"node4": cnd.exact_match("Ok, goodbye.")},
        },
        "node4": {RESPONSE: "bye", TRANSITIONS: {"node1": cnd.exact_match("Hi")}},
        "fallback_node": {
            RESPONSE: "Ooops",
            TRANSITIONS: {"node1": cnd.exact_match("Hi")},
        },
    }
}

actor = Actor(script, start_label=("greeting_flow", "start_node"), fallback_label=("greeting_flow", "fallback_node"))


def preprocess(ctx: Context, actor: Actor) -> Context:
    print(f"    preprocession Service (defined as an dict)")
    return ctx


def postprocess(ctx: Context, actor: Actor) -> Context:
    print(f"    postprocession Service (defined as a dict)")
    return ctx


provider = CallbackProvider()

pipeline = {
    "provider": provider,
    "connector": {},
    "services": [
        {
            "service": preprocess,
            "timeout": 1000
        },
        {
            "service": actor,
            "name": "encapsulated-actor"
        },
        Service(
            service=postprocess,
            name="postprocess",
            timeout=2000,
            start_condition=service_successful_condition("preprocess")
        )
    ]
}


@app.route('/df_provider')
def route():
    req = request.args.get('request')
    return provider.on_request(req)


if __name__ == '__main__':
    Pipeline(**pipeline).start()
    app.run()
    # Navigate to http://127.0.0.1:5000/df_provider?request=Hi
