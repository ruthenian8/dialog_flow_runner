from df_engine.core import Context, Actor

from df_runner import Pipeline

from examples import basic_example


def get_some_intent(ctx: Context, actor: Actor):
    ctx.misc["some_detection"] = {ctx.last_request: "some_intent"}


def get_another_intent(ctx: Context, actor: Actor):
    ctx.misc["another_detection"] = {ctx.last_request: "another_intent"}


def print_misc(ctx: Context, actor: Actor):
    print(f"{ctx.misc=}")


pipeline = Pipeline.from_script(
    basic_example.SCRIPT,
    start_label=("greeting_flow", "start_node"),
    fallback_label=("greeting_flow", "fallback_node"),
    pre_annotators=[get_some_intent, get_another_intent],
    post_annotators=[print_misc],
)


if __name__ == "__main__":
    pipeline.start_sync()
