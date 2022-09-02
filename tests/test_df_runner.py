from typing import List, Tuple, Callable

from df_engine.core import Context

from df_runner import Pipeline
import importlib

import pytest


# Uncomment the following line, if you want to run your examples during the test suite or import from them
# pytest.skip(allow_module_level=True)

import pathlib


TURNS = [
    ("Hi", "Hi, how are you?"),
    ("i'm fine, how are you?", "Good. What do you want to talk about?"),
    ("Let's talk about music.", "Sorry, I can not talk about music now."),
    ("Ok, goodbye.", "bye"),
    ("Hi", "Hi, how are you?"),
]


def run_pipeline_test(pipeline: Pipeline, turns: List[Tuple[str, str]]):
    ctx = Context()
    for turn_id, (request, true_response) in enumerate(turns):
        ctx = pipeline(request, ctx.id)
        if true_response != ctx.last_response:
            msg = f" pipeline={pipeline}"
            msg += f" turn_id={turn_id}"
            msg += f" request={request} "
            msg += f"\ntrue_response != out_response: "
            msg += f"\n{true_response} != {ctx.last_response}"
            raise Exception(msg)


def run_pipeline_test_wrapping_response_into_html(pipeline: Pipeline, wrapper: Callable[[str], str]):
    wrapped_turns = [(request, wrapper(response)) for request, response in TURNS]
    run_pipeline_test(pipeline, wrapped_turns)


@pytest.mark.parametrize(
    "module_path", [file for file in pathlib.Path("examples").glob("*.py") if not file.stem.startswith("_")]
)
def test_examples(module_path):
    module = importlib.import_module(f"examples.{module_path.stem}")
    try:
        if module_path.stem.startswith("6"):
            run_pipeline_test_wrapping_response_into_html(module.pipeline, module.construct_webpage_by_response)
        else:
            run_pipeline_test(module.pipeline, TURNS)
    except Exception as exc:
        raise Exception(f"model_name={module_path.stem}") from exc
