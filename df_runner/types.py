from typing import Callable

from df_engine.core import Context, Actor


ServiceFunctionType = Callable[[Context, Actor], (Context, bool)]

ServiceConditionType = Callable[[Context], bool]
