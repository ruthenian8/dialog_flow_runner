from examples import basic_example
import importlib

import pytest


# uncomment the following line, if you want to run your examples during the test suite or import from them

# pytest.skip(allow_module_level=True)

import pathlib

pathlib.Path("examples").glob("*.py")


@pytest.mark.parametrize("module_path", pathlib.Path("examples").glob("*.py"))
def test_examples(module_path):
    if module_path.stem in ["__init__", "wrappers_usage", "run_with_several_services"]:
        return
    module = importlib.import_module(f"examples.{module_path.stem}")
    try:
        basic_example.test_pipeline(module.pipeline)
    except Exception as exc:
        raise Exception(f"model_name={module_path.stem}") from exc