from examples import basic_example
import importlib

import pytest


# uncomment the following line, if you want to run your examples during the test suite or import from them

# pytest.skip(allow_module_level=True)

import pathlib

pathlib.Path("examples").glob("*.py")


@pytest.mark.parametrize("module_path", pathlib.Path("examples").glob("*.py"))
def test_examples(module_path):
    if module_path.stem in "__init__":
        return
    module = importlib.import_module(f"examples.{module_path.stem}")
    basic_example.test_pipeline(module.pipeline)