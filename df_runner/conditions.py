from df_engine.core import Actor, Context

from df_runner import ServiceCondition


def always_start_condition(ctx: Context, actor: Actor) -> Context:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return ctx


def service_successful_condition(name: str) -> ServiceCondition:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service
    """
    def internal(ctx: Context, actor: Actor) -> bool:
        return ctx.misc.get(f"{name}-success", False)
    return internal
