from typing import Any, Dict, Optional

from df_engine.core import Context

from .types import RUNNER_STATE_KEY


class StateTracker:

    def __init__(self, name: str):
        self.name = name

    def _set_state(self, ctx: Context, value: Any = None):
        if RUNNER_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.framework_states[RUNNER_STATE_KEY][self.name] = value

    def _get_state(self, ctx: Context, default: Optional[Any] = None) -> Dict:
        return ctx.framework_states[RUNNER_STATE_KEY].get(self.name, {} if default is None else default)
