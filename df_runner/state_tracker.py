from typing import Any, Optional, Dict

from df_engine.core import Context
from pydantic import BaseModel

from .types import RUNNER_STATE_KEY


class StateTracker(BaseModel):
    name: Optional[str] = None

    def _set_state(self, ctx: Context, value: Any = None):
        if RUNNER_STATE_KEY not in ctx.framework_states:
            ctx.framework_states[RUNNER_STATE_KEY] = {}
        ctx.framework_states[RUNNER_STATE_KEY][self.name] = value

    def _get_state(self, ctx: Context, default: Any = {}) -> Dict:
        return ctx.framework_states[RUNNER_STATE_KEY].get(self.name, default)