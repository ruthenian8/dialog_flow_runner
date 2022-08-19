from df_engine.core.script import Context

from df_runner import Pipeline
import importlib

import pytest


# uncomment the following line, if you want to run your examples during the test suite or import from them

# pytest.skip(allow_module_level=True)

import pathlib

pathlib.Path("examples").glob("*.py")


TURNS = [
    ("Hi", "Hi, how are you?"),
    ("i'm fine, how are you?", "Good. What do you want to talk about?"),
    ("Let's talk about music.", "Sorry, I can not talk about music now."),
    ("Ok, goodbye.", "bye"),
    ("Hi", "Hi, how are you?"),
]


def t_pipeline(pipeline: Pipeline):
    for turn_id, (request, true_response) in enumerate(TURNS):
        ctx: Context = pipeline(request)
        if true_response != ctx.last_response:
            msg = f" turn_id={turn_id}"
            msg += f" request={request} "
            msg += f"\ntrue_response != out_response: "
            msg += f"\n{true_response} != {ctx.last_response}"
            raise Exception(msg)


@pytest.mark.parametrize("module_path", pathlib.Path("examples").glob("*.py"))
def test_examples(module_path):
    if module_path.stem == "__init__":
        return
    module = importlib.import_module(f"examples.{module_path.stem}")
    try:
        t_pipeline(module.pipeline)
    except Exception as exc:
        raise Exception(f"model_name={module_path.stem}") from exc
