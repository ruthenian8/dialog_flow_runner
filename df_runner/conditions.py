from typing import Optional

from df_engine.core import Actor, Context

from df_runner import ServiceCondition, ConditionState, ServiceState


def always_start_condition(ctx: Context, actor: Actor) -> ConditionState:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return ConditionState.ALLOWED


def service_successful_condition(service: Optional[str] = None, group: Optional[str] = None) -> ServiceCondition:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service
    """
    if service is not None and group is not None:
        raise Exception(f"In one of the conditions both service ({service}) and group ({group}) were defined!")
    if service is None and group is None:
        raise Exception(f"In one of the conditions none service ({service}) nor group ({group}) were defined!")

    def check_service_state(name: str, ctx: Context):
        state = ctx.framework_states['RUNNER'].get(name, ServiceState.NOT_RUN)
        if state.value < 3:
            return ConditionState.WAITING
        elif state == ServiceState.FINISHED:
            return ConditionState.ALLOWED
        else:
            return ConditionState.DENIED

    def internal(ctx: Context, actor: Actor) -> ConditionState:
        if service is not None:
            return check_service_state(service, ctx)
        if group is not None:
            state = [check_service_state(serv, ctx) for serv in ctx.framework_states['SERVICES'][group]]
            if ConditionState.DENIED in state:
                return ConditionState.DENIED
            elif ConditionState.WAITING in state:
                return ConditionState.WAITING
            else:
                return ConditionState.ALLOWED

    return internal
