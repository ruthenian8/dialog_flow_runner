from typing import Optional

from df_engine.core import Actor, Context

from .types import (
    PIPELINE_STATE_KEY,
    StartConditionCheckerFunction,
    ComponentExecutionState,
    StartConditionCheckerAggregationFunction,
)


def always_start_condition(_: Context, __: Actor) -> bool:
    """
    Condition that always allows service execution, it's the default condition for all services.
    """
    return True


def service_successful_condition(name: Optional[str] = None) -> StartConditionCheckerFunction:
    """
    Condition that allows service execution, only if the other service was executed successfully.
    :name: - the name of the condition service or group
    """

    def check_service_state(ctx: Context, _: Actor):
        """
        Function that checks single service ServiceState and returns ConditionState for this service.
        """

        state = ctx.framework_states[PIPELINE_STATE_KEY].get(name, ComponentExecutionState.NOT_RUN)
        return state == ComponentExecutionState.FINISHED

    return check_service_state


def not_condition(function: StartConditionCheckerFunction) -> StartConditionCheckerFunction:
    def not_fun(ctx: Context, actor: Actor):
        return not function(ctx, actor)

    return not_fun


def aggregate_condition(
    aggregator: StartConditionCheckerAggregationFunction, *functions: StartConditionCheckerFunction
) -> StartConditionCheckerFunction:
    def aggregation_fun(ctx: Context, actor: Actor):
        return aggregator([function(ctx, actor) for function in functions])

    return aggregation_fun


def all_condition(*functions: StartConditionCheckerFunction) -> StartConditionCheckerFunction:
    return aggregate_condition(all, *functions)


def any_condition(*functions: StartConditionCheckerFunction) -> StartConditionCheckerFunction:
    return aggregate_condition(any, *functions)
