from typing import Optional

from df_engine.core import Actor, Context

from .types import FrameworkKeys, ConditionState, ServiceCondition, ServiceState


def always_start_condition(ctx: Context, actor: Actor) -> ConditionState:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return ConditionState.ALLOWED


def service_successful_condition(name: Optional[str] = None) -> ServiceCondition:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service or group
    """

    def check_service_state(ctx: Context, actor: Actor):
        """
        Function that checks single service ServiceState and returns ConditionState for this service.
        """

        state = ctx.framework_states[FrameworkKeys.RUNNER].get(name, ServiceState.NOT_RUN)
        if state not in (ServiceState.FINISHED, ServiceState.FAILED):
            return ConditionState.PENDING
        elif state == ServiceState.FINISHED:
            return ConditionState.ALLOWED
        else:
            return ConditionState.DENIED

    return check_service_state
