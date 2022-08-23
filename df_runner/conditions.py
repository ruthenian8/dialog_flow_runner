from typing import Optional

from df_engine.core import Actor, Context

from .types import RUNNER_STATE_KEY, StartConditionState, StartConditionCheckerFunction, PipeExecutionState


def always_start_condition(ctx: Context, actor: Actor) -> StartConditionState:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return StartConditionState.ALLOWED


def service_successful_condition(name: Optional[str] = None) -> StartConditionCheckerFunction:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service or group
    """

    def check_service_state(ctx: Context, actor: Actor):
        """
        Function that checks single service ServiceState and returns ConditionState for this service.
        """

        state = ctx.framework_states[RUNNER_STATE_KEY].get(name, PipeExecutionState.NOT_RUN)
        if state not in (PipeExecutionState.FINISHED, PipeExecutionState.FAILED):
            return StartConditionState.PENDING
        elif state == PipeExecutionState.FINISHED:
            return StartConditionState.ALLOWED
        else:
            return StartConditionState.DENIED

    return check_service_state
