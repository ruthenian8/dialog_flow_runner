from df_runner import Pipeline
from examples import basic_example

pipeline = Pipeline.from_script(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
)


if __name__ == "__main__":
    pipeline.start_sync()
