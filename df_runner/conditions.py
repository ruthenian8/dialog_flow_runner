from df_engine.core import Actor, Context

from df_runner import ServiceCondition


def always_start_condition(ctx: Context, actor: Actor) -> bool:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return True


def service_successful_condition(name: str) -> ServiceCondition:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service
    """
    def internal(context: Context, actor: Actor) -> bool:
        return context.framework_states['RUNNER'].get(f'{name}-success', False)
    return internal


def service_finished_condition(name: str) -> ServiceCondition:
    """
    Condition that allows service execution, only if the other service was executed (successfully or not).
    :name: - the name of the condition service
    """
    def internal(context: Context, actor: Actor) -> bool:
        return context.framework_states['RUNNER'].get(f'{name}-finished', False)
    return internal
